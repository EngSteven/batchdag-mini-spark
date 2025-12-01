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

	"github.com/google/uuid"
)

// --- LOGGING ESTRUCTURADO ---

type LogEntry struct {
	Level   string                 `json:"level"`
	Message string                 `json:"message"`
	Context map[string]interface{} `json:"context,omitempty"`
	Time    time.Time              `json:"time"`
}

func logJSON(level, msg string, ctx map[string]interface{}) {
	entry := LogEntry{
		Level:   level,
		Message: msg,
		Context: ctx,
		Time:    time.Now(),
	}
	json.NewEncoder(os.Stdout).Encode(entry)
}

// --- ESTRUCTURA MASTER ---

type Master struct {
	Workers map[string]*common.WorkerInfo
	Jobs    map[string]*common.Job

	// Estado de ejecución
	JobProgress map[string]map[string]string // JobID -> NodeID -> Status
	JobOutputs  map[string]map[string]string // JobID -> NodeID -> OutputPath
	JobFailures map[string]int               // JobID -> Cantidad de fallos

	// Tolerancia a fallos
	TaskQueue       chan common.Task
	TaskAssignments map[string]string      // TaskID -> WorkerID
	RunningTasks    map[string]common.Task // TaskID -> Task (Backup para reintentos)

	WorkerKeys []string
	rrIndex    int
	mu         sync.Mutex
}

// --- HANDLERS BÁSICOS ---

func (m *Master) registerHandler(w http.ResponseWriter, r *http.Request) {
	var req common.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	workerURL := fmt.Sprintf("http://localhost:%d", req.Port)
	m.Workers[req.ID] = &common.WorkerInfo{
		ID:            req.ID,
		URL:           workerURL,
		LastHeartbeat: time.Now(),
		Status:        "UP",
	}

	logJSON("INFO", "Worker registrado", map[string]interface{}{"worker_id": req.ID, "url": workerURL})
	w.WriteHeader(http.StatusOK)
}

func (m *Master) heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	var req common.HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	if worker, exists := m.Workers[req.ID]; exists {
		worker.LastHeartbeat = time.Now()
		worker.Status = "UP"
		worker.Metrics = req.Metrics // <--- Actualizamos métricas reales (CPU/RAM)
	}
	m.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

// --- GESTIÓN DE JOBS ---

func (m *Master) submitJobHandler(w http.ResponseWriter, r *http.Request) {
	var req common.JobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}

	jobID := uuid.New().String()
	job := &common.Job{
		ID:        jobID,
		Name:      req.Name,
		Status:    "RUNNING",
		Graph:     req.DAG,
		Submitted: time.Now(),
	}

	m.mu.Lock()
	m.Jobs[jobID] = job
	m.initJobProgress(job)
	m.mu.Unlock()

	logJSON("INFO", "Job recibido", map[string]interface{}{"job_id": jobID, "name": req.Name})
	go m.scheduleSourceTasks(job)

	json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "status": "ACCEPTED"})
}

// GET /api/v1/jobs/{id} Y /api/v1/jobs/{id}/results
func (m *Master) getJobStatusHandler(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "ID no encontrado", http.StatusBadRequest)
		return
	}
	jobID := parts[4]

	// Verificar si piden /results
	if len(parts) >= 6 && parts[5] == "results" {
		m.getJobResultsHandler(w, r, jobID)
		return
	}

	m.mu.Lock()
	job, exists := m.Jobs[jobID]
	
	// Copiar datos para evitar race conditions
	progressMap := make(map[string]string)
	completedCount := 0
	totalNodes := 0

	if p, ok := m.JobProgress[jobID]; ok {
		totalNodes = len(p)
		for k, v := range p {
			progressMap[k] = v
			if v == "COMPLETED" {
				completedCount++
			}
		}
	}
	failures := m.JobFailures[jobID]
	m.mu.Unlock()

	if !exists {
		http.Error(w, "Job no encontrado", http.StatusNotFound)
		return
	}

	duration := time.Since(job.Submitted).Seconds()
	if job.Status == "COMPLETED" || job.Status == "FAILED" {
		duration = job.Completed.Sub(job.Submitted).Seconds()
	}

	// Calcular porcentaje de progreso
	progressPercent := 0.0
	if totalNodes > 0 {
		progressPercent = (float64(completedCount) / float64(totalNodes)) * 100
	}

	resp := common.JobStatusResponse{
		ID:           job.ID,
		Name:         job.Name,
		Status:       job.Status,
		Submitted:    job.Submitted,
		DurationSecs: duration,
		Progress:     progressPercent, // <--- Dato real calculado
		NodeStatus:   progressMap,
		Failures:     failures,        // <--- Dato real de fallos
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Helper para obtener resultados finales (sinks del grafo)
func (m *Master) getJobResultsHandler(w http.ResponseWriter, r *http.Request, jobID string) {
	m.mu.Lock()
	job, exists := m.Jobs[jobID]
	outputs := make(map[string]string)
	
	if outs, ok := m.JobOutputs[jobID]; ok {
		for k, v := range outs {
			outputs[k] = v
		}
	}
	m.mu.Unlock()

	if !exists {
		http.Error(w, "Job no encontrado", http.StatusNotFound)
		return
	}

	// Identificar nodos "sink" (hojas del grafo que no tienen hijos)
	// 1. Contar out-degree (cuántas flechas salen de cada nodo)
	outDegree := make(map[string]int)
	for _, node := range job.Graph.Nodes {
		outDegree[node.ID] = 0
	}
	for _, edge := range job.Graph.Edges {
		outDegree[edge[0]]++ // Origen tiene un hijo
	}

	// 2. Seleccionar solo los nodos con out-degree 0
	finalOutputs := make(map[string]string)
	for nodeID, count := range outDegree {
		if count == 0 {
			if path, ok := outputs[nodeID]; ok {
				finalOutputs[nodeID] = path
			}
		}
	}

	resp := common.JobResultsResponse{
		JobID:   jobID,
		Outputs: finalOutputs,
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// --- PLANIFICADOR Y TOLERANCIA A FALLOS ---

func (m *Master) scheduleSourceTasks(job *common.Job) {
	inDegree := make(map[string]int)
	for _, node := range job.Graph.Nodes {
		inDegree[node.ID] = 0
	}
	for _, edge := range job.Graph.Edges {
		inDegree[edge[1]]++
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, node := range job.Graph.Nodes {
		if inDegree[node.ID] == 0 {
			m.queueTask(job.ID, node, []string{})
		}
	}
}

func (m *Master) queueTask(jobID string, node common.DAGNode, inputs []string) {
	m.setNodeStatus(jobID, node.ID, "SCHEDULED")
	
	task := common.Task{
		ID:         uuid.New().String(),
		JobID:      jobID,
		NodeID:     node.ID,
		Op:         node.Op,
		Fn:         node.Fn,
		Args:       []string{node.Path},
		InputFiles: inputs,
		Attempt:    1,
	}
	m.TaskQueue <- task
	logJSON("INFO", "Tarea encolada", map[string]interface{}{"task_id": task.ID, "node": node.ID})
}

func (m *Master) schedulerLoop() {
	for task := range m.TaskQueue {
		m.mu.Lock()
		
		// Filtrar workers UP
		var availableWorkers []*common.WorkerInfo
		for _, w := range m.Workers {
			if w.Status == "UP" {
				availableWorkers = append(availableWorkers, w)
			}
		}

		if len(availableWorkers) == 0 {
			m.mu.Unlock()
			// fmt.Println("[MASTER] Esperando workers...") // Reducir ruido
			time.Sleep(2 * time.Second)
			m.TaskQueue <- task
			continue
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
		logJSON("WARN", "Error enviando a worker", map[string]interface{}{"worker": worker.ID, "error": err.Error()})
		m.mu.Lock()
		delete(m.TaskAssignments, task.ID)
		m.mu.Unlock()
		m.TaskQueue <- task
		return
	}
	defer resp.Body.Close()
}

func (m *Master) completeTaskHandler(w http.ResponseWriter, r *http.Request) {
	var res common.TaskResult
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.TaskAssignments, res.ID)
	originalTask, taskFound := m.RunningTasks[res.ID]
	delete(m.RunningTasks, res.ID)

	// --- MANEJO DE FALLOS ---
	if res.Status == "FAILED" {
		m.JobFailures[res.JobID]++ // Registrar fallo
		logJSON("ERROR", "Fallo en tarea", map[string]interface{}{"node": res.NodeID, "error": res.ErrorMsg})
		
		if taskFound && originalTask.Attempt < common.MaxRetries {
			originalTask.Attempt++
			originalTask.ID = uuid.New().String()
			logJSON("INFO", "Reintentando tarea", map[string]interface{}{"node": res.NodeID, "attempt": originalTask.Attempt})
			
			go func(t common.Task) { m.TaskQueue <- t }(originalTask)
		} else {
			logJSON("ERROR", "Tarea excedió reintentos. Job fallido.", map[string]interface{}{"node": res.NodeID})
			if job, ok := m.Jobs[res.JobID]; ok {
				job.Status = "FAILED"
				job.Completed = time.Now()
			}
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	// --- ÉXITO ---
	m.setNodeStatus(res.JobID, res.NodeID, "COMPLETED")
	
	if _, ok := m.JobOutputs[res.JobID]; !ok {
		m.JobOutputs[res.JobID] = make(map[string]string)
	}
	m.JobOutputs[res.JobID][res.NodeID] = res.Result
	
	logJSON("INFO", "Tarea completada", map[string]interface{}{"node": res.NodeID})

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
				if m.getNodeStatus(job.ID, edge[0]) != "COMPLETED" {
					allParentsDone = false
					break
				}
				inputFiles = append(inputFiles, m.JobOutputs[job.ID][edge[0]])
			}
		}

		if hasParents && allParentsDone {
			m.queueTask(job.ID, node, inputFiles)
		}
	}
}

func (m *Master) checkJobCompletion(job *common.Job) {
	allDone := true
	for _, node := range job.Graph.Nodes {
		if m.getNodeStatus(job.ID, node.ID) != "COMPLETED" {
			allDone = false
			break
		}
	}
	if allDone {
		logJSON("INFO", "Job completado exitosamente", map[string]interface{}{"job_id": job.ID})
		job.Status = "COMPLETED"
		job.Completed = time.Now()
	}
}

// --- MONITOR DE SALUD ---

func (m *Master) healthCheckLoop() {
	for {
		time.Sleep(5 * time.Second)
		m.mu.Lock()
		now := time.Now()
		
		for wID, w := range m.Workers {
			if w.Status == "UP" && now.Sub(w.LastHeartbeat) > 10*time.Second {
				logJSON("ALERT", "Worker detectado muerto", map[string]interface{}{"worker_id": wID})
				w.Status = "DOWN"
				
				// Recuperación
				for tID, workerAssigned := range m.TaskAssignments {
					if workerAssigned == wID {
						if task, ok := m.RunningTasks[tID]; ok {
							logJSON("INFO", "Re-planificando tarea huérfana", map[string]interface{}{"node": task.NodeID})
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

// --- HELPERS ---

func (m *Master) initJobProgress(job *common.Job) {
	if _, ok := m.JobProgress[job.ID]; !ok {
		m.JobProgress[job.ID] = make(map[string]string)
	}
	if _, ok := m.JobOutputs[job.ID]; !ok {
		m.JobOutputs[job.ID] = make(map[string]string)
	}
	if _, ok := m.JobFailures[job.ID]; !ok {
		m.JobFailures[job.ID] = 0 // Inicializar contador de fallos
	}
	for _, node := range job.Graph.Nodes {
		m.JobProgress[job.ID][node.ID] = "PENDING"
	}
}

func (m *Master) getNodeStatus(jobID, nodeID string) string {
	if s, ok := m.JobProgress[jobID]; ok {
		return s[nodeID]
	}
	return "PENDING"
}

func (m *Master) setNodeStatus(jobID, nodeID, status string) {
	if _, ok := m.JobProgress[jobID]; !ok {
		m.JobProgress[jobID] = make(map[string]string)
	}
	m.JobProgress[jobID][nodeID] = status
}

func main() {
	master := &Master{
		Workers:         make(map[string]*common.WorkerInfo),
		Jobs:            make(map[string]*common.Job),
		JobProgress:     make(map[string]map[string]string),
		JobOutputs:      make(map[string]map[string]string),
		JobFailures:     make(map[string]int), // <--- Inicializar mapa de fallos
		TaskQueue:       make(chan common.Task, 100),
		TaskAssignments: make(map[string]string),
		RunningTasks:    make(map[string]common.Task),
	}

	http.HandleFunc("/register", master.registerHandler)
	http.HandleFunc("/heartbeat", master.heartbeatHandler)
	http.HandleFunc("/api/v1/jobs", master.submitJobHandler)
	http.HandleFunc("/api/v1/jobs/", master.getJobStatusHandler) // Maneja status y results
	http.HandleFunc("/task/complete", master.completeTaskHandler)

	go master.healthCheckLoop()
	go master.schedulerLoop()

	logJSON("INFO", "Master iniciado", map[string]interface{}{"port": 8080, "version": "1.0"})
	log.Fatal(http.ListenAndServe(":8080", nil))
}