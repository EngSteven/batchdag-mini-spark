package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"mini-spark/internal/common"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Estado global del Master
type Master struct {
	Workers map[string]*common.WorkerInfo
	Jobs    map[string]*common.Job

	// Mapa de progreso: JobID -> NodeID -> Estatus ("PENDING", "SCHEDULED", "COMPLETED")
	JobProgress map[string]map[string]string
	JobOutputs map[string]map[string]string

	TaskQueue  chan common.Task // Canal para tareas pendientes
	WorkerKeys []string         // Lista auxiliar para Round-Robin eficiente
	rrIndex    int              // Índice actual para Round-Robin
	mu         sync.Mutex
}

// --- HANDLERS DE REGISTRO Y HEARTBEAT ---

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

	fmt.Printf("[MASTER] Worker registrado: %s en %s\n", req.ID, workerURL)
	w.WriteHeader(http.StatusOK)
}

func (m *Master) heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	var req common.HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if worker, exists := m.Workers[req.ID]; exists {
		worker.LastHeartbeat = time.Now()
		worker.Status = "UP"
	} else {
		// fmt.Printf("[MASTER] Heartbeat de worker desconocido: %s\n", req.ID)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// --- HANDLERS DE TRABAJO (JOBS & TASKS) ---

func (m *Master) submitJobHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
		return
	}

	var req common.JobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}

	jobID := uuid.New().String()
	job := &common.Job{
		ID:        jobID,
		Name:      req.Name,
		Status:    "ACCEPTED",
		Graph:     req.DAG,
		Submitted: time.Now(),
	}

	m.mu.Lock()
	m.Jobs[jobID] = job
	m.initJobProgress(job) // Inicializar estados en PENDING
	m.mu.Unlock()

	fmt.Printf("[MASTER] Job recibido: %s (ID: %s)\n", req.Name, jobID)

	// Iniciar planificación de las tareas fuente (sin dependencias)
	go m.scheduleSourceTasks(job)

	// Responder con el ID del job
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"job_id": jobID,
		"status": "ACCEPTED",
	})
}

func (m *Master) completeTaskHandler(w http.ResponseWriter, r *http.Request) {
	var res common.TaskResult
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Marcar nodo como completado usando el helper
	// Esto corrige el error de tipos (bool vs string)
	m.setNodeStatus(res.JobID, res.NodeID, "COMPLETED")
	fmt.Printf("[MASTER] Tarea completada: %s (Node: %s)\n", res.ID, res.NodeID)

	// 2. Verificar dependencias y planificar siguientes tareas
	job, exists := m.Jobs[res.JobID]
	if !exists {
		return
	}

	// Guardar el output de la tarea
	if _, ok := m.JobOutputs[res.JobID]; !ok {
			m.JobOutputs[res.JobID] = make(map[string]string)
	}
	m.JobOutputs[res.JobID][res.NodeID] = res.Result

	// Luego marcar como COMPLETED y llamar a dependencias...
	m.setNodeStatus(res.JobID, res.NodeID, "COMPLETED")

	m.checkAndScheduleDependents(job)

	w.WriteHeader(http.StatusOK)
}

// --- LÓGICA DEL PLANIFICADOR (SCHEDULER) ---

// Encuentra nodos que no tienen dependencias y crea tareas
func (m *Master) scheduleSourceTasks(job *common.Job) {
	// Calcular indegree
	inDegree := make(map[string]int)
	for _, node := range job.Graph.Nodes {
		inDegree[node.ID] = 0
	}
	for _, edge := range job.Graph.Edges {
		dest := edge[1]
		inDegree[dest]++
	}

	// Como esta funcion se llama desde una goroutine aparte, necesitamos lockear
	// para modificar el estado a SCHEDULED safely
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, node := range job.Graph.Nodes {
		if inDegree[node.ID] == 0 {
			m.setNodeStatus(job.ID, node.ID, "SCHEDULED")

			task := common.Task{
				ID:        uuid.New().String(),
				JobID:     job.ID,
				NodeID:    node.ID,
				Op:        node.Op,
				Fn:        node.Fn,
				Args:      []string{node.Path},
				Partition: 0,
			}
			fmt.Printf("[MASTER] Planificando tarea fuente: %s (Node: %s)\n", task.ID, node.ID)
			m.TaskQueue <- task
		}
	}
}

// Revisa si los hijos de las tareas completadas ya pueden ejecutarse
func (m *Master) checkAndScheduleDependents(job *common.Job) {
	// NOTA: Se asume que m.mu ya está bloqueado por quien llama a esta función (completeTaskHandler)

	for _, node := range job.Graph.Nodes {
		// Si ya está agendada o completa, saltar
		status := m.getNodeStatus(job.ID, node.ID)
		if status == "COMPLETED" || status == "SCHEDULED" {
			continue
		}

		// Verificar padres
		allParentsDone := true
		hasParents := false

		for _, edge := range job.Graph.Edges {
			source, dest := edge[0], edge[1]
			if dest == node.ID {
				hasParents = true
				parentStatus := m.getNodeStatus(job.ID, source)
				if parentStatus != "COMPLETED" {
					allParentsDone = false
					break
				}
			}
		}

		// Si tiene padres y todos terminaron, agendar
		if hasParents && allParentsDone {
			m.setNodeStatus(job.ID, node.ID, "SCHEDULED")

			// RECOLECTAR INPUTS DE LOS PADRES
			var inputFiles []string
			for _, edge := range job.Graph.Edges {
					source, dest := edge[0], edge[1]
					if dest == node.ID {
							// Buscar el archivo que generó el padre
							if path, ok := m.JobOutputs[job.ID][source]; ok {
									inputFiles = append(inputFiles, path)
							}
					}
			}

			task := common.Task{
				ID:     uuid.New().String(),
				JobID:  job.ID,
				NodeID: node.ID,
				Op:     node.Op,
				Fn:     node.Fn,
				InputFiles: inputFiles,
			}
			fmt.Printf("[MASTER] Dependencias satisfechas -> Encolando: %s (Inputs: %v)\n", node.ID, inputFiles)		
			m.TaskQueue <- task
		}
	}
}

// Bucle principal: saca tareas de la cola y las manda a workers
func (m *Master) schedulerLoop() {
	for task := range m.TaskQueue {
		m.mu.Lock()

		// 1. Filtrar workers UP
		var availableWorkers []*common.WorkerInfo
		for _, w := range m.Workers {
			if w.Status == "UP" {
				availableWorkers = append(availableWorkers, w)
			}
		}

		if len(availableWorkers) == 0 {
			m.mu.Unlock()
			fmt.Println("[MASTER] No hay workers. Esperando...")
			time.Sleep(2 * time.Second)
			m.TaskQueue <- task // Reencolar
			continue
		}

		// 2. Round-Robin
		worker := availableWorkers[m.rrIndex%len(availableWorkers)]
		m.rrIndex++
		m.mu.Unlock()

		// 3. Enviar (en goroutine para no bloquear el loop)
		go m.sendTask(worker, task)
	}
}

func (m *Master) sendTask(worker *common.WorkerInfo, task common.Task) {
	data, _ := json.Marshal(task)
	resp, err := http.Post(worker.URL+"/task", "application/json", bytes.NewBuffer(data))

	if err != nil {
		fmt.Printf("[MASTER] Error enviando a %s: %v\n", worker.ID, err)
		// Aquí deberíamos reencolar la tarea en caso de fallo real
		return
	}
	defer resp.Body.Close()
	// fmt.Printf("[MASTER] Tarea enviada a %s\n", worker.ID)
}

// --- HELPERS DE ESTADO Y MONITOR ---

func (m *Master) initJobProgress(job *common.Job) {
	if _, ok := m.JobProgress[job.ID]; !ok {
		m.JobProgress[job.ID] = make(map[string]string)
	}
	for _, node := range job.Graph.Nodes {
		m.JobProgress[job.ID][node.ID] = "PENDING"
	}
}

func (m *Master) getNodeStatus(jobID, nodeID string) string {
	if states, ok := m.JobProgress[jobID]; ok {
		if status, ok := states[nodeID]; ok {
			return status
		}
	}
	return "PENDING"
}

func (m *Master) setNodeStatus(jobID, nodeID, status string) {
	if _, ok := m.JobProgress[jobID]; !ok {
		m.JobProgress[jobID] = make(map[string]string)
	}
	m.JobProgress[jobID][nodeID] = status
}

func (m *Master) healthCheckLoop() {
	for {
		time.Sleep(5 * time.Second)
		m.mu.Lock()
		now := time.Now()
		for id, w := range m.Workers {
			if now.Sub(w.LastHeartbeat) > 10*time.Second {
				if w.Status == "UP" {
					fmt.Printf("[MASTER] ALERTA: Worker %s marcado como DOWN\n", id)
					w.Status = "DOWN"
				}
			}
		}
		m.mu.Unlock()
	}
}

func main() {
	master := &Master{
		Workers:     make(map[string]*common.WorkerInfo),
		Jobs:        make(map[string]*common.Job),
		JobProgress: make(map[string]map[string]string), // Inicializado correctamente
		JobOutputs: make(map[string]map[string]string),
		TaskQueue:   make(chan common.Task, 100),
	}

	// Endpoints
	http.HandleFunc("/register", master.registerHandler)
	http.HandleFunc("/heartbeat", master.heartbeatHandler)
	http.HandleFunc("/api/v1/jobs", master.submitJobHandler)
	http.HandleFunc("/task/complete", master.completeTaskHandler)

	// Loops en background
	go master.healthCheckLoop()
	go master.schedulerLoop()

	fmt.Println("[MASTER] Escuchando en puerto 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}