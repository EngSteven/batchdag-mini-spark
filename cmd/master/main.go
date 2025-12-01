package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"mini-spark/internal/common"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	"net"

	"github.com/google/uuid"
)

// --- LOGGING ---
type LogEntry struct {
	Level   string                 `json:"level"`
	Message string                 `json:"message"`
	Context map[string]interface{} `json:"context,omitempty"`
	Time    time.Time              `json:"time"`
}

func logJSON(level, msg string, ctx map[string]interface{}) {
	entry := LogEntry{Level: level, Message: msg, Context: ctx, Time: time.Now()}
	json.NewEncoder(os.Stdout).Encode(entry)
}

// --- MASTER ---
type Master struct {
	Workers map[string]*common.WorkerInfo
	Jobs    map[string]*common.Job

	JobProgress map[string]map[string]string
	JobOutputs  map[string]map[string]string
	JobFailures map[string]int

	TaskQueue       chan common.Task
	TaskAssignments map[string]string
	RunningTasks    map[string]common.Task

	WorkerKeys []string
	rrIndex    int
	mu         sync.Mutex
	
	stateFile string // Archivo de persistencia
}

// --- PERSISTENCIA (NUEVO) ---

// Guarda el estado de los Jobs en disco
func (m *Master) saveState() {
	// Guardamos solo lo crítico: Jobs, Outputs, Failures
	data := struct {
		Jobs        map[string]*common.Job
		JobOutputs  map[string]map[string]string
		JobFailures map[string]int
	}{
		Jobs:        m.Jobs,
		JobOutputs:  m.JobOutputs,
		JobFailures: m.JobFailures,
	}

	file, err := os.Create(m.stateFile)
	if err != nil {
		logJSON("ERROR", "No se pudo guardar estado", map[string]interface{}{"error": err.Error()})
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		logJSON("ERROR", "Error serializando estado", map[string]interface{}{"error": err.Error()})
	}
}

// Carga el estado al inicio
func (m *Master) loadState() {
	file, err := os.Open(m.stateFile)
	if err != nil {
		logJSON("INFO", "Iniciando sin estado previo", nil)
		return
	}
	defer file.Close()

	data := struct {
		Jobs        map[string]*common.Job
		JobOutputs  map[string]map[string]string
		JobFailures map[string]int
	}{}

	if err := json.NewDecoder(file).Decode(&data); err != nil {
		logJSON("ERROR", "Archivo de estado corrupto", map[string]interface{}{"error": err.Error()})
		return
	}

	m.Jobs = data.Jobs
	m.JobOutputs = data.JobOutputs
	m.JobFailures = data.JobFailures

	// Reconstruir JobProgress basado en el grafo y status del Job
	// (Simplificación: si el job estaba completado, marcamos todo completed)
	for _, job := range m.Jobs {
		m.initJobProgress(job)
		if job.Status == "COMPLETED" {
			for _, node := range job.Graph.Nodes {
				m.setNodeStatus(job.ID, node.ID, "COMPLETED")
			}
		}
	}
	logJSON("INFO", "Estado recuperado", map[string]interface{}{"jobs_loaded": len(m.Jobs)})
}


// --- HANDLERS ---

func (m *Master) registerHandler(w http.ResponseWriter, r *http.Request) {
	var req common.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// DETECCIÓN INTELIGENTE DE IP
	// Obtenemos la IP de origen de la petición HTTP
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// Fallback si no se puede parsear (ej: tests locales raros)
		host = "localhost"
	}
	
	// Si es local, preferimos "localhost" para evitar problemas de DNS raros en WSL
	if host == "::1" || host == "127.0.0.1" {
		host = "localhost"
	}

	// Construimos la URL real usando la IP detectada y el puerto reportado
	workerURL := fmt.Sprintf("http://%s:%d", host, req.Port)

	m.Workers[req.ID] = &common.WorkerInfo{
		ID:            req.ID,
		URL:           workerURL,
		LastHeartbeat: time.Now(),
		Status:        "UP",
	}

	logJSON("INFO", "Worker registrado", map[string]interface{}{
		"worker_id": req.ID, 
		"ip_detected": host,
		"url": workerURL,
	})
	w.WriteHeader(http.StatusOK)
}

func (m *Master) heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	var req common.HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil { w.WriteHeader(http.StatusBadRequest); return }
	m.mu.Lock()
	if worker, exists := m.Workers[req.ID]; exists {
		worker.LastHeartbeat = time.Now(); worker.Status = "UP"; worker.Metrics = req.Metrics
	}
	m.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (m *Master) submitJobHandler(w http.ResponseWriter, r *http.Request) {
	var req common.JobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil { http.Error(w, "JSON inválido", http.StatusBadRequest); return }

	jobID := uuid.New().String()
	job := &common.Job{ID: jobID, Name: req.Name, Status: "RUNNING", Graph: req.DAG, Submitted: time.Now()}

	m.mu.Lock()
	m.Jobs[jobID] = job
	m.initJobProgress(job)
	m.saveState() // <--- Guardar estado
	m.mu.Unlock()

	logJSON("INFO", "Job recibido", map[string]interface{}{"job_id": jobID})
	go m.scheduleSourceTasks(job)
	json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "status": "ACCEPTED"})
}

func (m *Master) getJobStatusHandler(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 { http.Error(w, "ID no encontrado", http.StatusBadRequest); return }
	jobID := parts[4]
	if len(parts) >= 6 && parts[5] == "results" { m.getJobResultsHandler(w, r, jobID); return }

	m.mu.Lock()
	job, exists := m.Jobs[jobID]
	progressMap := make(map[string]string)
	completedCount := 0
	totalNodes := 0
	if p, ok := m.JobProgress[jobID]; ok {
		totalNodes = len(p)
		for k, v := range p {
			progressMap[k] = v
			if v == "COMPLETED" { completedCount++ }
		}
	}
	failures := m.JobFailures[jobID]
	m.mu.Unlock()

	if !exists { http.Error(w, "Job no encontrado", http.StatusNotFound); return }
	duration := time.Since(job.Submitted).Seconds()
	if job.Status == "COMPLETED" || job.Status == "FAILED" { duration = job.Completed.Sub(job.Submitted).Seconds() }
	
	progressPercent := 0.0
	if totalNodes > 0 { progressPercent = (float64(completedCount) / float64(totalNodes)) * 100 }

	resp := common.JobStatusResponse{
		ID: job.ID, Name: job.Name, Status: job.Status, Submitted: job.Submitted,
		DurationSecs: duration, Progress: progressPercent, NodeStatus: progressMap, Failures: failures,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (m *Master) getJobResultsHandler(w http.ResponseWriter, r *http.Request, jobID string) {
	m.mu.Lock()
	_, exists := m.Jobs[jobID]
	outputs := make(map[string]string)
	if outs, ok := m.JobOutputs[jobID]; ok { for k, v := range outs { outputs[k] = v } }
	m.mu.Unlock()

	if !exists { http.Error(w, "Job no encontrado", http.StatusNotFound); return }

	outDegree := make(map[string]int)
	for _, node := range m.Jobs[jobID].Graph.Nodes { outDegree[node.ID] = 0 }
	for _, edge := range m.Jobs[jobID].Graph.Edges { outDegree[edge[0]]++ }

	finalOutputs := make(map[string]string)
	for nodeID, count := range outDegree {
		if count == 0 { if path, ok := outputs[nodeID]; ok { finalOutputs[nodeID] = path } }
	}
	json.NewEncoder(w).Encode(common.JobResultsResponse{JobID: jobID, Outputs: finalOutputs})
}

// --- CORE ---

func (m *Master) scheduleSourceTasks(job *common.Job) {
	inDegree := make(map[string]int)
	for _, node := range job.Graph.Nodes { inDegree[node.ID] = 0 }
	for _, edge := range job.Graph.Edges { inDegree[edge[1]]++ }

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, node := range job.Graph.Nodes {
		if inDegree[node.ID] == 0 { m.queueTask(job.ID, node, []string{}) }
	}
}

func (m *Master) queueTask(jobID string, node common.DAGNode, inputs []string) {
	m.setNodeStatus(jobID, node.ID, "SCHEDULED")
	task := common.Task{
		ID: uuid.New().String(), JobID: jobID, NodeID: node.ID, Op: node.Op, Fn: node.Fn,
		Args: []string{node.Path}, InputFiles: inputs, Attempt: 1,
	}
	m.TaskQueue <- task
	logJSON("INFO", "Tarea encolada", map[string]interface{}{"task_id": task.ID, "node": node.ID})
}

func (m *Master) schedulerLoop() {
	for task := range m.TaskQueue {
		m.mu.Lock()
		var availableWorkers []*common.WorkerInfo
		for _, w := range m.Workers { if w.Status == "UP" { availableWorkers = append(availableWorkers, w) } }

		if len(availableWorkers) == 0 {
			m.mu.Unlock(); time.Sleep(2 * time.Second); m.TaskQueue <- task; continue
		}

		worker := availableWorkers[m.rrIndex%len(availableWorkers)]
		m.rrIndex++
		m.TaskAssignments[task.ID] = worker.ID
		m.RunningTasks[task.ID] = task
		m.mu.Unlock()
		go m.sendTask(worker, task)
	}
}

func (m *Master) sendTask(worker *common.WorkerInfo, task common.Task) {
	data, _ := json.Marshal(task)
	resp, err := http.Post(worker.URL+"/task", "application/json", bytes.NewBuffer(data))
	if err != nil {
		m.mu.Lock(); delete(m.TaskAssignments, task.ID); m.mu.Unlock()
		m.TaskQueue <- task; return
	}
	defer resp.Body.Close()
}

func (m *Master) completeTaskHandler(w http.ResponseWriter, r *http.Request) {
	var res common.TaskResult
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil { http.Error(w, "Bad Request", http.StatusBadRequest); return }

	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.TaskAssignments, res.ID)
	originalTask, taskFound := m.RunningTasks[res.ID]
	delete(m.RunningTasks, res.ID)

	if res.Status == "FAILED" {
		m.JobFailures[res.JobID]++
		logJSON("ERROR", "Fallo en tarea", map[string]interface{}{"node": res.NodeID, "error": res.ErrorMsg})
		if taskFound && originalTask.Attempt < common.MaxRetries {
			originalTask.Attempt++
			originalTask.ID = uuid.New().String()
			go func(t common.Task) { m.TaskQueue <- t }(originalTask)
		} else {
			if job, ok := m.Jobs[res.JobID]; ok { job.Status = "FAILED"; job.Completed = time.Now() }
		}
		m.saveState() // <--- Guardar estado tras fallo
		w.WriteHeader(http.StatusOK)
		return
	}

	m.setNodeStatus(res.JobID, res.NodeID, "COMPLETED")
	if _, ok := m.JobOutputs[res.JobID]; !ok { m.JobOutputs[res.JobID] = make(map[string]string) }
	m.JobOutputs[res.JobID][res.NodeID] = res.Result
	logJSON("INFO", "Tarea completada", map[string]interface{}{"node": res.NodeID})
	
	m.saveState() // <--- Guardar estado tras éxito

	if job, ok := m.Jobs[res.JobID]; ok {
		m.checkAndScheduleDependents(job)
		m.checkJobCompletion(job)
	}
	w.WriteHeader(http.StatusOK)
}

func (m *Master) checkAndScheduleDependents(job *common.Job) {
	for _, node := range job.Graph.Nodes {
		if m.getNodeStatus(job.ID, node.ID) != "PENDING" { continue }
		allParentsDone := true
		hasParents := false
		var inputFiles []string
		for _, edge := range job.Graph.Edges {
			if edge[1] == node.ID {
				hasParents = true
				if m.getNodeStatus(job.ID, edge[0]) != "COMPLETED" { allParentsDone = false; break }
				inputFiles = append(inputFiles, m.JobOutputs[job.ID][edge[0]])
			}
		}
		if hasParents && allParentsDone { m.queueTask(job.ID, node, inputFiles) }
	}
}

func (m *Master) checkJobCompletion(job *common.Job) {
	allDone := true
	for _, node := range job.Graph.Nodes {
		if m.getNodeStatus(job.ID, node.ID) != "COMPLETED" { allDone = false; break }
	}
	if allDone {
		logJSON("INFO", "Job completado", map[string]interface{}{"job_id": job.ID})
		job.Status = "COMPLETED"
		job.Completed = time.Now()
		m.saveState() // <--- Guardar estado final
	}
}

func (m *Master) healthCheckLoop() {
	for {
		time.Sleep(5 * time.Second)
		m.mu.Lock()
		now := time.Now()
		for wID, w := range m.Workers {
			if w.Status == "UP" && now.Sub(w.LastHeartbeat) > 10*time.Second {
				logJSON("ALERT", "Worker muerto", map[string]interface{}{"worker_id": wID})
				w.Status = "DOWN"
				for tID, workerAssigned := range m.TaskAssignments {
					if workerAssigned == wID {
						if task, ok := m.RunningTasks[tID]; ok {
							delete(m.TaskAssignments, tID)
							task.ID = uuid.New().String()
							delete(m.RunningTasks, tID)
							go func(t common.Task) { m.TaskQueue <- t }(task)
						}
					}
				}
			}
		}
		m.mu.Unlock()
	}
}

func (m *Master) initJobProgress(job *common.Job) {
	if _, ok := m.JobProgress[job.ID]; !ok { m.JobProgress[job.ID] = make(map[string]string) }
	if _, ok := m.JobOutputs[job.ID]; !ok { m.JobOutputs[job.ID] = make(map[string]string) }
	if _, ok := m.JobFailures[job.ID]; !ok { m.JobFailures[job.ID] = 0 }
	for _, node := range job.Graph.Nodes { m.JobProgress[job.ID][node.ID] = "PENDING" }
}

func (m *Master) getNodeStatus(jobID, nodeID string) string {
	if s, ok := m.JobProgress[jobID]; ok { return s[nodeID] }
	return "PENDING"
}

func (m *Master) setNodeStatus(jobID, nodeID, status string) {
	if _, ok := m.JobProgress[jobID]; !ok { m.JobProgress[jobID] = make(map[string]string) }
	m.JobProgress[jobID][nodeID] = status
}

func main() {
	master := &Master{
		Workers:         make(map[string]*common.WorkerInfo),
		Jobs:            make(map[string]*common.Job),
		JobProgress:     make(map[string]map[string]string),
		JobOutputs:      make(map[string]map[string]string),
		JobFailures:     make(map[string]int),
		TaskQueue:       make(chan common.Task, 100),
		TaskAssignments: make(map[string]string),
		RunningTasks:    make(map[string]common.Task),
		stateFile:       "master_state.json", // Archivo local
	}

	master.loadState() // <--- Cargar estado al inicio

	http.HandleFunc("/register", master.registerHandler)
	http.HandleFunc("/heartbeat", master.heartbeatHandler)
	http.HandleFunc("/api/v1/jobs", master.submitJobHandler)
	http.HandleFunc("/api/v1/jobs/", master.getJobStatusHandler)
	http.HandleFunc("/task/complete", master.completeTaskHandler)

	go master.healthCheckLoop()
	go master.schedulerLoop()

	logJSON("INFO", "Master iniciado con persistencia", map[string]interface{}{"port": 8080})
	log.Fatal(http.ListenAndServe(":8080", nil))
}