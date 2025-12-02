/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: types.go
Descripcion: Definiciones de tipos y estructuras compartidas entre
             todos los componentes de Mini-Spark. Incluye modelos
             de datos para Jobs, DAGs, Tareas, Workers, Metricas,
             y mensajes de coordinacion (registro, heartbeat, resultados).
*/

package common

import "time"

// Numero maximo de reintentos para tareas fallidas
const (
	MaxRetries = 3
)

// --- Métricas y Observabilidad ---

// SystemMetrics contiene datos de rendimiento del nodo
// Usado para monitoreo de salud y scheduling inteligente
type SystemMetrics struct {
	CPUPercent  float64 `json:"cpu_usage"`    // Uso de CPU simulado o real (0-100)
	MemoryUsage uint64  `json:"memory_usage"` // Bytes en uso (Alloc del runtime)
	ActiveTasks int     `json:"active_tasks"` // Número de tareas ejecutando concurrentemente
}

// --- Estructuras de Coordinación ---

// WorkerInfo representa un nodo trabajador registrado en el Master
// Mantiene estado de conectividad y metricas de rendimiento
type WorkerInfo struct {
	ID            string        `json:"id"`             // UUID del worker
	URL           string        `json:"url"`            // Endpoint HTTP (http://host:port)
	LastHeartbeat time.Time     `json:"last_heartbeat"` // Timestamp del ultimo heartbeat
	Status        string        `json:"status"`         // "UP" o "DOWN"
	Metrics       SystemMetrics `json:"metrics"`        // Metricas actuales del worker
}

// RegisterRequest es el JSON que envía el worker al iniciar
// para registrarse en el cluster
type RegisterRequest struct {
	ID   string `json:"id"`   // UUID autogenerado del worker
	Port int    `json:"port"` // Puerto donde escucha el worker
}

// HeartbeatRequest señal de vida con métricas
// Enviado periodicamente (cada 3s) por workers al Master
type HeartbeatRequest struct {
	ID      string        `json:"id"`      // UUID del worker
	Metrics SystemMetrics `json:"metrics"` // Metricas actuales (CPU, memoria, tareas)
}

// --- Estructuras de Jobs y Tareas ---

// JobRequest mapea el JSON enviado por el cliente al submitir un job
type JobRequest struct {
	Name        string `json:"name"`        // Nombre descriptivo del job
	DAG         DAG    `json:"dag"`         // Grafo dirigido aciclico de operaciones
	Parallelism int    `json:"parallelism"` // Nivel de paralelismo deseado (no usado actualmente)
}

// DAG representa el grafo de ejecucion del job
type DAG struct {
	Nodes []DAGNode  `json:"nodes"` // Lista de nodos (operaciones)
	Edges [][]string `json:"edges"` // Aristas [source_id, dest_id]
}

// DAGNode representa una operacion individual en el DAG
type DAGNode struct {
	ID         string `json:"id"`                   // Identificador unico del nodo
	Op         string `json:"op"`                   // Tipo de operacion (read_csv, map, reduce_by_key, etc)
	Fn         string `json:"fn,omitempty"`         // Nombre de funcion UDF (para map/filter)
	Path       string `json:"path,omitempty"`       // Ruta de archivo (para read_csv)
	Partitions int    `json:"partitions,omitempty"` // Numero de particiones (no usado actualmente)
	Key        string `json:"key,omitempty"`        // Columna clave (para join)
}

// Job representa un trabajo distribuido en ejecucion
type Job struct {
	ID        string    `json:"id"`                     // UUID del job
	Name      string    `json:"name"`                   // Nombre descriptivo
	Status    string    `json:"status"`                 // RUNNING | COMPLETED | FAILED
	Graph     DAG       `json:"dag"`                    // DAG de operaciones
	Submitted time.Time `json:"submitted_at"`           // Timestamp de envio
	Completed time.Time `json:"completed_at,omitempty"` // Timestamp de finalizacion
}

// Task representa una unidad de trabajo asignada a un worker
type Task struct {
	ID         string   `json:"id"`          // UUID de la tarea
	JobID      string   `json:"job_id"`      // Job al que pertenece
	NodeID     string   `json:"node_id"`     // Nodo del DAG correspondiente
	Op         string   `json:"op"`          // Operacion a ejecutar
	Fn         string   `json:"fn"`          // Funcion UDF (si aplica)
	Args       []string `json:"args"`        // Argumentos (ej: path de archivo)
	InputFiles []string `json:"input_files"` // Archivos de entrada (outputs de nodos padre)
	Partition  int      `json:"partition"`   // Numero de particion (no usado)
	Attempt    int      `json:"attempt"`     // Contador de reintentos (1-3)
}

// TaskResult mensaje enviado por worker al completar/fallar una tarea
type TaskResult struct {
	ID       string `json:"id"`                  // UUID de la tarea
	JobID    string `json:"job_id"`              // Job al que pertenece
	NodeID   string `json:"node_id"`             // Nodo del DAG
	Status   string `json:"status"`              // COMPLETED | FAILED
	Result   string `json:"result"`              // Ruta del archivo de salida
	ErrorMsg string `json:"error_msg,omitempty"` // Mensaje de error si fallo
}

// --- Respuestas de API ---

// JobStatusResponse enriquecido con progreso y métricas
// Devuelto por GET /api/v1/jobs/{id}
type JobStatusResponse struct {
	ID           string            `json:"id"`               // UUID del job
	Name         string            `json:"name"`             // Nombre del job
	Status       string            `json:"status"`           // Estado actual
	Submitted    time.Time         `json:"submitted_at"`     // Timestamp de envio
	DurationSecs float64           `json:"duration_secs"`    // Duracion en segundos
	Progress     float64           `json:"progress_percent"` // Porcentaje de avance (0-100)
	NodeStatus   map[string]string `json:"node_status"`      // Estado por nodo: PENDING|SCHEDULED|COMPLETED
	Failures     int               `json:"failure_count"`    // Contador total de fallos
}

// JobResultsResponse para la descarga de resultados finales
// Devuelto por GET /api/v1/jobs/{id}/results
type JobResultsResponse struct {
	JobID   string            `json:"job_id"`  // UUID del job
	Outputs map[string]string `json:"outputs"` // Mapa NodeID -> ruta de archivo de salida
}
