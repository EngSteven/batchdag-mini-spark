package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"mini-spark/internal/common"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Master struct {
	Workers map[string]*common.WorkerInfo
	Jobs    map[string]*common.Job

	// Estado de ejecución
	JobProgress map[string]map[string]string // JobID -> NodeID -> Status
	JobOutputs  map[string]map[string]string // JobID -> NodeID -> OutputPath

	// Tolerancia a fallos
	TaskQueue       chan common.Task
	TaskAssignments map[string]string      // TaskID -> WorkerID (Quién la tiene asignada)
	RunningTasks    map[string]common.Task // TaskID -> Task (Copia de la tarea para re-encolar)

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
	fmt.Printf("[MASTER] Worker registrado: %s\n", req.ID)
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

	fmt.Printf("[MASTER] Job recibido: %s (%s)\n", req.Name, jobID)
	go m.scheduleSourceTasks(job)

	json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "status": "ACCEPTED"})
}

func (m *Master) getJobStatusHandler(w http.ResponseWriter, r *http.Request) {
	// Extraer ID de la URL: /api/v1/jobs/{id}
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "ID no encontrado", http.StatusBadRequest)
		return
	}
	jobID := parts[4]

	m.mu.Lock()
	job, exists := m.Jobs[jobID]
	progress := make(map[string]string)
	if p, ok := m.JobProgress[jobID]; ok {
		// Copiar mapa para evitar condiciones de carrera al codificar JSON
		for k, v := range p {
			progress[k] = v
		}
	}
	m.mu.Unlock()

	if !exists {
		http.Error(w, "Job no encontrado", http.StatusNotFound)
		return
	}

	duration := time.Since(job.Submitted).Seconds()
	if job.Status == "COMPLETED" || job.Status == "FAILED" {
		duration = job.Completed.Sub(job.Submitted).Seconds()
	}

	resp := common.JobStatusResponse{
		ID:           job.ID,
		Name:         job.Name,
		Status:       job.Status,
		Submitted:    job.Submitted,
		DurationSecs: duration,
		Progress:     progress,
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

// Helper para crear y encolar tarea
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
	fmt.Printf("[MASTER] Tarea encolada: %s (Node: %s)\n", task.ID, node.ID)
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
			fmt.Println("[MASTER] Esperando workers...")
			time.Sleep(2 * time.Second)
			m.TaskQueue <- task
			continue
		}

		worker := availableWorkers[m.rrIndex%len(availableWorkers)]
		m.rrIndex++
		
		// REGISTRAR ASIGNACIÓN (CRÍTICO PARA TOLERANCIA A FALLOS)
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
		fmt.Printf("[MASTER] Error enviando a %s. Reencolando...\n", worker.ID)
		m.mu.Lock()
		delete(m.TaskAssignments, task.ID) // Limpiar asignación fallida
		// No borramos de RunningTasks porque se va a re-encolar
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

	// Limpiar tracking de ejecución
	delete(m.TaskAssignments, res.ID)
	originalTask, taskFound := m.RunningTasks[res.ID]
	delete(m.RunningTasks, res.ID)

	// MANEJO DE FALLOS Y REINTENTOS
	if res.Status == "FAILED" {
		fmt.Printf("[MASTER] Tarea %s FALLÓ (Error: %s)\n", res.NodeID, res.ErrorMsg)
		
		if taskFound && originalTask.Attempt < common.MaxRetries {
			originalTask.Attempt++
			originalTask.ID = uuid.New().String() // Nuevo ID de tarea para el reintento
			fmt.Printf("[MASTER] Reintentando tarea %s (Intento %d/%d)\n", res.NodeID, originalTask.Attempt, common.MaxRetries)
			
			// Re-encolar (necesitamos liberar lock antes de enviar a channel si fuera blocking, pero TaskQueue tiene buffer)
			// Para seguridad, lo hacemos en goroutine o aseguramos buffer. 
			// Aquí asumimos buffer suficiente o enviamos a goroutine.
			go func(t common.Task) { m.TaskQueue <- t }(originalTask)
		} else {
			fmt.Printf("[MASTER] Tarea %s excedió reintentos. Job FAILED.\n", res.NodeID)
			if job, ok := m.Jobs[res.JobID]; ok {
				job.Status = "FAILED"
				job.Completed = time.Now()
			}
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	// ÉXITO
	m.setNodeStatus(res.JobID, res.NodeID, "COMPLETED")
	
	if _, ok := m.JobOutputs[res.JobID]; !ok {
		m.JobOutputs[res.JobID] = make(map[string]string)
	}
	m.JobOutputs[res.JobID][res.NodeID] = res.Result
	
	fmt.Printf("[MASTER] Tarea completada: %s\n", res.NodeID)

	// Verificar dependencias
	if job, ok := m.Jobs[res.JobID]; ok {
		m.checkAndScheduleDependents(job)
		m.checkJobCompletion(job)
	}
	w.WriteHeader(http.StatusOK)
}

func (m *Master) checkAndScheduleDependents(job *common.Job) {
	// (Lógica similar a la anterior, optimizada)
	for _, node := range job.Graph.Nodes {
		status := m.getNodeStatus(job.ID, node.ID)
		if status != "PENDING" { continue }

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
		fmt.Printf("[MASTER] JOB %s COMPLETADO EXITOSAMENTE!\n", job.ID)
		job.Status = "COMPLETED"
		job.Completed = time.Now()
	}
}

// --- MONITOR DE SALUD (Heartbeats & Recovery) ---

func (m *Master) healthCheckLoop() {
	for {
		time.Sleep(5 * time.Second)
		m.mu.Lock()
		now := time.Now()
		
		for wID, w := range m.Workers {
			if w.Status == "UP" && now.Sub(w.LastHeartbeat) > 10*time.Second {
				fmt.Printf("[MASTER] ALERTA: Worker %s murió. Iniciando recuperación...\n", wID)
				w.Status = "DOWN"
				
				// RECUPERACIÓN: Buscar tareas asignadas a este worker muerto
				for tID, workerAssigned := range m.TaskAssignments {
					if workerAssigned == wID {
						if task, ok := m.RunningTasks[tID]; ok {
							fmt.Printf("[MASTER] Re-planificando tarea %s (víctima de worker caído)\n", task.NodeID)
							// Eliminar asignación vieja
							delete(m.TaskAssignments, tID)
							// Generar nuevo ID para evitar colisiones lógicas
							task.ID = uuid.New().String() 
							delete(m.RunningTasks, tID) // Borrar vieja
							
							// Re-encolar en background
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
		TaskQueue:       make(chan common.Task, 100),
		TaskAssignments: make(map[string]string),
		RunningTasks:    make(map[string]common.Task),
	}

	http.HandleFunc("/register", master.registerHandler)
	http.HandleFunc("/heartbeat", master.heartbeatHandler)
	http.HandleFunc("/api/v1/jobs", master.submitJobHandler)
	http.HandleFunc("/api/v1/jobs/", master.getJobStatusHandler) // endpoint con ID
	http.HandleFunc("/task/complete", master.completeTaskHandler)

	go master.healthCheckLoop()
	go master.schedulerLoop()

	fmt.Println("[MASTER] Mini-Spark Master v1.0 listo en puerto 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}