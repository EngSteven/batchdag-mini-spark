/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: api.go
Descripcion: Handlers HTTP del nodo Master para API REST.
             Implementa endpoints de registro de workers, heartbeats,
             envio de jobs, consulta de estado/resultados, y reporte
             de tareas completadas. Coordina flujo de datos distribuido.
*/

package master

import (
	"encoding/json"
	"fmt"
	"mini-spark/internal/common"
	"mini-spark/internal/utils"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// RegisterHandler - Registra un worker en el cluster
// Entrada: w - response writer, r - request con RegisterRequest JSON
// Salida: HTTP 200 OK o 400 Bad Request
// Descripcion: Procesa solicitud de registro de worker, extrae IP remota,
//
//	construye URL del worker y lo agrega al pool de workers.
//	Inicializa estado UP y timestamp de heartbeat.
func (m *Master) RegisterHandler(w http.ResponseWriter, r *http.Request) {
	var req common.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Extraer IP del cliente desde RemoteAddr
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = "localhost"
	}
	// Normalizar localhost IPv6/IPv4
	if host == "::1" || host == "127.0.0.1" {
		host = "localhost"
	}

	// Construir URL completa del worker
	workerURL := fmt.Sprintf("http://%s:%d", host, req.Port)
	// Registrar worker en mapa global
	m.Workers[req.ID] = &common.WorkerInfo{
		ID:            req.ID,
		URL:           workerURL,
		LastHeartbeat: time.Now(),
		Status:        "UP",
	}

	utils.LogJSON("INFO", "Worker registrado", map[string]interface{}{"worker_id": req.ID, "url": workerURL})
	w.WriteHeader(http.StatusOK)
}

// HeartbeatHandler - Procesa heartbeats de workers
// Entrada: w - response writer, r - request con HeartbeatRequest JSON
// Salida: HTTP 200 OK o 400 Bad Request
// Descripcion: Actualiza timestamp de ultimo heartbeat y metricas del worker.
//
//	Marca worker como UP. Permite detectar workers caidos.
func (m *Master) HeartbeatHandler(w http.ResponseWriter, r *http.Request) {
	var req common.HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	// Actualizar estado del worker si existe
	if worker, exists := m.Workers[req.ID]; exists {
		worker.LastHeartbeat = time.Now() // Actualizar timestamp
		worker.Status = "UP"              // Reactivar si estaba DOWN
		worker.Metrics = req.Metrics      // Guardar metricas actuales
	}
	m.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

// SubmitJobHandler - Recibe y registra nuevos jobs para ejecucion
// Entrada: w - response writer, r - request con JobRequest JSON
// Salida: HTTP 200 con job_id o 400 Bad Request
// Descripcion: Parsea definicion de job (DAG), asigna UUID, inicializa
//
//	estado de progreso, persiste en disco y lanza scheduler
//	en goroutine separada para procesar nodos source.
func (m *Master) SubmitJobHandler(w http.ResponseWriter, r *http.Request) {
	var req common.JobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest)
		return
	}

	// Generar ID unico para el job
	jobID := uuid.New().String()
	// Crear objeto Job con estado inicial RUNNING
	job := &common.Job{ID: jobID, Name: req.Name, Status: "RUNNING", Graph: req.DAG, Parallelism: req.Parallelism ,Submitted: time.Now()}

	m.mu.Lock()
	// Registrar job en mapa global
	m.Jobs[jobID] = job
	// Inicializar mapas de progreso y outputs
	m.InitJobProgress(job)
	// Persistir estado a disco
	m.SaveState()
	m.mu.Unlock()

	utils.LogJSON("INFO", "Job recibido", map[string]interface{}{"job_id": jobID, "parellelism": job.Parallelism})
	// Lanzar scheduler para procesar nodos source (sin dependencias)
	go m.ScheduleSourceTasks(job)
	// Responder con ID del job
	json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "status": "ACCEPTED"})
}

// GetJobStatusHandler - Consulta estado y progreso de un job
// Entrada: w - response writer, r - request con job_id en URL
// Salida: HTTP 200 con JobStatusResponse o 400/404
// Descripcion: Extrae job_id de URL, calcula porcentaje de progreso,
//
//	duracion, y estado por nodo. Si URL termina en /results,
//	delega a GetJobResultsHandler.
func (m *Master) GetJobStatusHandler(w http.ResponseWriter, r *http.Request) {
	// Extraer job_id de la URL (/api/v1/jobs/{id})
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "ID no encontrado", http.StatusBadRequest)
		return
	}
	jobID := parts[4]
	// Detectar si se solicitan resultados (/api/v1/jobs/{id}/results)
	if len(parts) >= 6 && parts[5] == "results" {
		m.GetJobResultsHandler(w, r, jobID)
		return
	}

	m.mu.Lock()
	job, exists := m.Jobs[jobID]
	// Construir mapa de estado por nodo
	progressMap := make(map[string]string)
	completedCount := 0
	totalNodes := 0
	if p, ok := m.JobProgress[jobID]; ok {
		totalNodes = len(p)
		for k, v := range p {
			progressMap[k] = v
			// Contar nodos completados
			if v == "COMPLETED" {
				completedCount++
			}
		}
	}
	// Obtener contador de fallos
	failures := m.JobFailures[jobID]
	m.mu.Unlock()

	if !exists {
		http.Error(w, "Job no encontrado", http.StatusNotFound)
		return
	}
	// Calcular duracion desde envio hasta ahora o hasta completado
	duration := time.Since(job.Submitted).Seconds()
	if job.Status == "COMPLETED" || job.Status == "FAILED" {
		duration = job.Completed.Sub(job.Submitted).Seconds()
	}

	// Calcular porcentaje de progreso
	progressPercent := 0.0
	if totalNodes > 0 {
		progressPercent = (float64(completedCount) / float64(totalNodes)) * 100
	}

	json.NewEncoder(w).Encode(common.JobStatusResponse{
		ID: job.ID, Name: job.Name, Status: job.Status, Submitted: job.Submitted,
		DurationSecs: duration, Progress: progressPercent, NodeStatus: progressMap, Failures: failures,
	})
}

// GetJobResultsHandler - Devuelve archivos de salida finales de un job
// Entrada: w - response writer, r - request, jobID - ID del job
// Salida: HTTP 200 con JobResultsResponse o 404
// Descripcion: Identifica nodos sink (sin hijos en el DAG) y devuelve
//
//	mapa de rutas de archivos de salida. Solo incluye
//	resultados finales, no intermedios.
func (m *Master) GetJobResultsHandler(w http.ResponseWriter, r *http.Request, jobID string) {
	m.mu.Lock()
	_, exists := m.Jobs[jobID]
	// Copiar mapa de outputs
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

	// Calcular out-degree de cada nodo para identificar sinks
	outDegree := make(map[string]int)
	for _, node := range m.Jobs[jobID].Graph.Nodes {
		outDegree[node.ID] = 0
	}
	for _, edge := range m.Jobs[jobID].Graph.Edges {
		outDegree[edge[0]]++
	} // Incrementar out-degree del padre

	// Filtrar solo nodos sink (out-degree == 0)
	finalOutputs := make(map[string]string)
	for nodeID, count := range outDegree {
		if count == 0 { // Nodo sin hijos = resultado final
			if path, ok := outputs[nodeID]; ok {
				finalOutputs[nodeID] = path
			}
		}
	}
	json.NewEncoder(w).Encode(common.JobResultsResponse{JobID: jobID, Outputs: finalOutputs})
}

// CompleteTaskHandler - Procesa reporte de tareas completadas/fallidas
// Entrada: w - response writer, r - request con TaskResult JSON
// Salida: HTTP 200 OK o 400 Bad Request
// Descripcion: Actualiza estado de tarea, maneja reintentos en caso de fallo,
//
//	registra outputs exitosos, dispara scheduling de nodos
//	dependientes, y verifica si el job completo.
func (m *Master) CompleteTaskHandler(w http.ResponseWriter, r *http.Request) {
	var res common.TaskResult
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Eliminar asignacion de tarea
	delete(m.TaskAssignments, res.ID)
	
	// Recuperar tarea original (para reintentos)
	originalTask, taskFound := m.RunningTasks[res.ID]
	delete(m.RunningTasks, res.ID)

	// --- MANEJO DE FALLOS ---
	if res.Status == "FAILED" {
		m.JobFailures[res.JobID]++
		utils.LogJSON("ERROR", "Fallo en tarea", map[string]interface{}{
			"node": res.NodeID, 
			"part": res.PartitionID, 
			"error": res.ErrorMsg,
		})

		if taskFound && originalTask.Attempt < common.MaxRetries {
			originalTask.Attempt++
			originalTask.ID = uuid.New().String()
			go func(t common.Task) { m.TaskQueue <- t }(originalTask)
		} else {
			if job, ok := m.Jobs[res.JobID]; ok {
				job.Status = "FAILED"
				job.Completed = time.Now()
			}
		}
		m.SaveState()
		w.WriteHeader(http.StatusOK)
		return
	}

	// --- MANEJO DE ÉXITO ---

	// 1. Actualizar estado de la PARTICIÓN específica
	m.setPartitionStatus(res.JobID, res.NodeID, res.PartitionID, "COMPLETED")

	// 2. NUEVO: Verificar si TODAS las particiones del nodo terminaron
	// Esto es necesario para que el API muestre el nodo como "COMPLETED"
	if job, ok := m.Jobs[res.JobID]; ok {
		isNodeDone := true
		p := job.Parallelism
		if p < 1 { p = 1 }

		for i := 0; i < p; i++ {
			// Si alguna partición NO está completa, el nodo sigue corriendo
			if m.getPartitionStatus(res.JobID, res.NodeID, i) != "COMPLETED" {
				isNodeDone = false
				break
			}
		}

		// Si todas las particiones terminaron, marcamos el Nodo completo
		if isNodeDone {
			m.setNodeStatus(res.JobID, res.NodeID, "COMPLETED")
		}
	}

	// 3. Registrar Outputs
	if _, ok := m.JobPartitionOutputs[res.JobID]; !ok {
		m.JobPartitionOutputs[res.JobID] = make(map[string]map[int]string)
	}
	if _, ok := m.JobPartitionOutputs[res.JobID][res.NodeID]; !ok {
		m.JobPartitionOutputs[res.JobID][res.NodeID] = make(map[int]string)
	}
	m.JobPartitionOutputs[res.JobID][res.NodeID][res.PartitionID] = res.Result

	// Compatibilidad con CLI
	if _, ok := m.JobOutputs[res.JobID]; !ok {
		m.JobOutputs[res.JobID] = make(map[string]string)
	}
	m.JobOutputs[res.JobID][res.NodeID] = res.Result

	utils.LogJSON("INFO", "Tarea completada", map[string]interface{}{
		"node": res.NodeID, 
		"part": res.PartitionID,
	})

	m.SaveState()

	if job, ok := m.Jobs[res.JobID]; ok {
		m.CheckAndScheduleDependents(job)
		m.CheckJobCompletion(job)
	}
	w.WriteHeader(http.StatusOK)
}