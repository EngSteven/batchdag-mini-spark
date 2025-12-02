/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: scheduler.go
Descripcion: Planificador de tareas del nodo Master.
             Implementa algoritmo topologico para ordenar ejecucion de DAG,
             asignacion round-robin de tareas a workers, y deteccion de
             dependencias completadas para desbloquear nodos.
*/

package master

import (
	"bytes"
	"encoding/json"
	"mini-spark/internal/common"
	"mini-spark/internal/utils"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// ScheduleSourceTasks - Identifica y encola nodos source del DAG
// Entrada: job - puntero al Job a procesar
// Salida: ninguna (void)
// Descripcion: Calcula in-degree de cada nodo en el DAG.
//
//	Nodos con in-degree == 0 (sin dependencias) se encolan
//	inmediatamente. Primer paso del scheduling topologico.
func (m *Master) ScheduleSourceTasks(job *common.Job) {
	// Calcular in-degree (numero de aristas entrantes) para cada nodo
	inDegree := make(map[string]int)
	for _, node := range job.Graph.Nodes {
		inDegree[node.ID] = 0
	}
	for _, edge := range job.Graph.Edges {
		inDegree[edge[1]]++
	} // edge[1] es el destino

	m.mu.Lock()
	defer m.mu.Unlock()
	// Encolar todos los nodos sin dependencias (source nodes)
	for _, node := range job.Graph.Nodes {
		if inDegree[node.ID] == 0 {
			// Nodo source - encolar con lista de inputs vacia
			m.queueTask(job.ID, node, []string{})
		}
	}
}

// queueTask - Crea y encola una tarea para ejecucion
// Entrada: jobID - ID del job, node - nodo del DAG, inputs - archivos de entrada
// Salida: ninguna (void)
// Descripcion: Construye objeto Task, actualiza estado a SCHEDULED,
//
//	y lo inserta en TaskQueue para asignacion a workers.
func (m *Master) queueTask(jobID string, node common.DAGNode, inputs []string) {
	// Marcar nodo como programado
	m.setNodeStatus(jobID, node.ID, "SCHEDULED")
	// Construir objeto Task
	task := common.Task{
		ID: uuid.New().String(), JobID: jobID, NodeID: node.ID, Op: node.Op, Fn: node.Fn,
		Args: []string{node.Path}, InputFiles: inputs, Attempt: 1,
	}
	// Encolar tarea en canal bloqueante
	m.TaskQueue <- task
	utils.LogJSON("INFO", "Tarea encolada", map[string]interface{}{"task_id": task.ID, "node": node.ID})
}

// SchedulerLoop - Loop principal de asignacion de tareas a workers
// Entrada: ninguna (lee de TaskQueue)
// Salida: ninguna (void), loop infinito
// Descripcion: Consume tareas del TaskQueue, selecciona worker disponible
//
//	usando round-robin, registra asignacion y envia tarea via HTTP.
//	Si no hay workers, reencola tarea y espera.
func (m *Master) SchedulerLoop() {
	for task := range m.TaskQueue {
		m.mu.Lock()
		// Filtrar workers activos (estado UP)
		var availableWorkers []*common.WorkerInfo
		for _, w := range m.Workers {
			if w.Status == "UP" {
				availableWorkers = append(availableWorkers, w)
			}
		}

		// Si no hay workers disponibles, reencolar y esperar
		if len(availableWorkers) == 0 {
			m.mu.Unlock()
			time.Sleep(2 * time.Second)
			m.TaskQueue <- task
			continue
		}

		// Seleccionar worker usando round-robin
		worker := availableWorkers[m.rrIndex%len(availableWorkers)]
		m.rrIndex++ // Incrementar indice para siguiente asignacion
		// Registrar asignacion tarea-worker
		m.TaskAssignments[task.ID] = worker.ID
		m.RunningTasks[task.ID] = task
		m.mu.Unlock()
		// Enviar tarea al worker en goroutine separada
		go m.sendTask(worker, task)
	}
}

// sendTask - Envia tarea a worker via HTTP POST
// Entrada: worker - info del worker, task - tarea a enviar
// Salida: ninguna (void)
// Descripcion: Serializa Task a JSON y lo envia al endpoint /task del worker.
//
//	Si falla, reencola la tarea para reintento.
func (m *Master) sendTask(worker *common.WorkerInfo, task common.Task) {
	// Serializar tarea a JSON
	data, _ := json.Marshal(task)
	// Enviar POST a worker
	resp, err := http.Post(worker.URL+"/task", "application/json", bytes.NewBuffer(data))
	if err != nil {
		// Si falla, liberar asignacion y reencolar
		m.mu.Lock()
		delete(m.TaskAssignments, task.ID)
		m.mu.Unlock()
		m.TaskQueue <- task
		return
	}
	defer resp.Body.Close()
}

func (m *Master) CheckAndScheduleDependents(job *common.Job) {
	for _, node := range job.Graph.Nodes {
		if m.getNodeStatus(job.ID, node.ID) != "PENDING" {
			continue
		}
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

func (m *Master) CheckJobCompletion(job *common.Job) {
	allDone := true
	for _, node := range job.Graph.Nodes {
		if m.getNodeStatus(job.ID, node.ID) != "COMPLETED" {
			allDone = false
			break
		}
	}
	if allDone {
		utils.LogJSON("INFO", "Job completado", map[string]interface{}{"job_id": job.ID})
		job.Status = "COMPLETED"
		job.Completed = time.Now()
		m.SaveState()
	}
}

// HealthCheckLoop - Monitorea salud de workers y reasigna tareas caidas
// Entrada: ninguna
// Salida: ninguna (void), loop infinito
// Descripcion: Cada 5 segundos verifica timestamp de heartbeats.
//
//	Workers sin heartbeat por >10s se marcan DOWN.
//	Tareas asignadas a workers caidos se reencolan con nuevo ID.
func (m *Master) HealthCheckLoop() {
	for {
		time.Sleep(5 * time.Second)
		m.mu.Lock()
		now := time.Now()
		// Revisar cada worker registrado
		for wID, w := range m.Workers {
			// Si worker esta UP pero sin heartbeat reciente, marcarlo DOWN
			if w.Status == "UP" && now.Sub(w.LastHeartbeat) > 10*time.Second {
				utils.LogJSON("ALERT", "Worker muerto", map[string]interface{}{"worker_id": wID})
				w.Status = "DOWN"
				// Reencolar todas las tareas asignadas a este worker
				for tID, workerAssigned := range m.TaskAssignments {
					if workerAssigned == wID {
						if task, ok := m.RunningTasks[tID]; ok {
							// Liberar recursos
							delete(m.TaskAssignments, tID)
							// Generar nuevo ID para evitar conflictos
							task.ID = uuid.New().String()
							delete(m.RunningTasks, tID)
							// Reencolar tarea
							go func(t common.Task) { m.TaskQueue <- t }(task)
						}
					}
				}
			}
		}
		m.mu.Unlock()
	}
}
