/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: state.go
Descripcion: Gestion de estado persistente del nodo Master.
             Implementa serializacion/deserializacion de Jobs, outputs,
             y metricas a disco (JSON). Permite recuperacion ante fallos
             y reinicio del Master sin perder historial de jobs.
*/

package master

import (
	"encoding/json"
	"mini-spark/internal/common"
	"mini-spark/internal/utils"
	"os"
	"sync"
)

// Master representa el nodo coordinador central del sistema
type Master struct {
	Workers map[string]*common.WorkerInfo // Mapa de workers registrados (ID -> WorkerInfo)
	Jobs    map[string]*common.Job        // Mapa de jobs (ID -> Job)

	JobProgress map[string]map[string]string // Progreso por nodo: JobID -> NodeID -> Estado
	JobOutputs  map[string]map[string]string // Archivos de salida: JobID -> NodeID -> Path
	JobFailures map[string]int               // Contador de fallos: JobID -> Num fallos

	TaskQueue       chan common.Task       // Cola de tareas pendientes (buffered channel)
	TaskAssignments map[string]string      // Asignaciones activas: TaskID -> WorkerID
	RunningTasks    map[string]common.Task // Tareas en ejecucion: TaskID -> Task

	WorkerKeys []string   // Keys de workers (no usado actualmente)
	rrIndex    int        // Indice round-robin para asignacion de tareas
	mu         sync.Mutex // Mutex para concurrencia segura

	stateFile string // Ruta del archivo de persistencia JSON
}

// NewMaster - Constructor del nodo Master
// Entrada: stateFile - ruta del archivo de persistencia JSON
// Salida: puntero a instancia Master inicializada
// Descripcion: Inicializa mapas vacios, crea canal TaskQueue con buffer de 100,
//
//	y configura archivo de estado para SaveState/LoadState.
func NewMaster(stateFile string) *Master {
	return &Master{
		Workers:         make(map[string]*common.WorkerInfo),
		Jobs:            make(map[string]*common.Job),
		JobProgress:     make(map[string]map[string]string),
		JobOutputs:      make(map[string]map[string]string),
		JobFailures:     make(map[string]int),
		TaskQueue:       make(chan common.Task, 100), // Buffer de 100 tareas
		TaskAssignments: make(map[string]string),
		RunningTasks:    make(map[string]common.Task),
		stateFile:       stateFile,
	}
}

// InitJobProgress - Inicializa mapas de seguimiento para un nuevo job
// Entrada: job - puntero al Job recien creado
// Salida: ninguna (void)
// Descripcion: Crea mapas de progreso, outputs y fallos para el job.
//
//	Inicializa todos los nodos en estado PENDING.
func (m *Master) InitJobProgress(job *common.Job) {
	// Crear mapa de progreso si no existe
	if _, ok := m.JobProgress[job.ID]; !ok {
		m.JobProgress[job.ID] = make(map[string]string)
	}
	// Crear mapa de outputs si no existe
	if _, ok := m.JobOutputs[job.ID]; !ok {
		m.JobOutputs[job.ID] = make(map[string]string)
	}
	// Inicializar contador de fallos en 0
	if _, ok := m.JobFailures[job.ID]; !ok {
		m.JobFailures[job.ID] = 0
	}
	// Marcar todos los nodos como PENDING
	for _, node := range job.Graph.Nodes {
		m.JobProgress[job.ID][node.ID] = "PENDING"
	}
}

// getNodeStatus - Consulta estado actual de un nodo en un job
// Entrada: jobID - ID del job, nodeID - ID del nodo
// Salida: string con estado (PENDING|SCHEDULED|COMPLETED)
// Descripcion: Accede a mapa JobProgress para obtener estado.
//
//	Retorna "PENDING" si no existe el job o nodo.
func (m *Master) getNodeStatus(jobID, nodeID string) string {
	if s, ok := m.JobProgress[jobID]; ok {
		return s[nodeID]
	}
	return "PENDING"
}

// setNodeStatus - Actualiza estado de un nodo en un job
// Entrada: jobID - ID del job, nodeID - ID del nodo, status - nuevo estado
// Salida: ninguna (void)
// Descripcion: Actualiza mapa JobProgress. Crea mapa si no existe.
func (m *Master) setNodeStatus(jobID, nodeID, status string) {
	if _, ok := m.JobProgress[jobID]; !ok {
		m.JobProgress[jobID] = make(map[string]string)
	}
	m.JobProgress[jobID][nodeID] = status
}

// SaveState - Persiste estado del Master a disco en formato JSON
// Entrada: ninguna (usa this.stateFile)
// Salida: ninguna (void), loguea errores si falla
// Descripcion: Serializa Jobs, JobOutputs y JobFailures a archivo JSON.
//
//	Usa formato indentado para legibilidad. No persiste workers
//	ni tareas en ejecucion (son volatiles).
func (m *Master) SaveState() {
	// Estructura temporal para serializacion
	data := struct {
		Jobs        map[string]*common.Job
		JobOutputs  map[string]map[string]string
		JobFailures map[string]int
	}{
		Jobs:        m.Jobs,
		JobOutputs:  m.JobOutputs,
		JobFailures: m.JobFailures,
	}

	// Crear archivo de estado
	file, err := os.Create(m.stateFile)
	if err != nil {
		utils.LogJSON("ERROR", "No se pudo guardar estado", map[string]interface{}{"error": err.Error()})
		return
	}
	defer file.Close()

	// Serializar con formato indentado
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		utils.LogJSON("ERROR", "Error serializando estado", map[string]interface{}{"error": err.Error()})
	}
}

// LoadState - Recupera estado del Master desde disco
// Entrada: ninguna (usa this.stateFile)
// Salida: ninguna (void), inicializa sin estado si archivo no existe
// Descripcion: Deserializa JSON de estado, restaura Jobs, JobOutputs y JobFailures.
//
//	Reinicializa JobProgress para jobs recuperados.
//	Marca jobs completados con todos sus nodos COMPLETED.
func (m *Master) LoadState() {
	// Intentar abrir archivo de estado
	file, err := os.Open(m.stateFile)
	if err != nil {
		utils.LogJSON("INFO", "Iniciando sin estado previo", nil)
		return
	}
	defer file.Close()

	// Estructura temporal para deserializacion
	data := struct {
		Jobs        map[string]*common.Job
		JobOutputs  map[string]map[string]string
		JobFailures map[string]int
	}{}

	// Deserializar JSON
	if err := json.NewDecoder(file).Decode(&data); err != nil {
		utils.LogJSON("ERROR", "Archivo de estado corrupto", map[string]interface{}{"error": err.Error()})
		return
	}

	// Restaurar estado
	m.Jobs = data.Jobs
	m.JobOutputs = data.JobOutputs
	m.JobFailures = data.JobFailures

	// Reconstruir mapas de progreso
	for _, job := range m.Jobs {
		m.InitJobProgress(job)
		// Si el job estaba completado, marcar todos sus nodos
		if job.Status == "COMPLETED" {
			for _, node := range job.Graph.Nodes {
				m.setNodeStatus(job.ID, node.ID, "COMPLETED")
			}
		}
	}
	utils.LogJSON("INFO", "Estado recuperado", map[string]interface{}{"jobs_loaded": len(m.Jobs)})
}
