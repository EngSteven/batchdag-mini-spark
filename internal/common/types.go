package common

import "time"

const (
	MaxRetries = 3
)

// --- Métricas y Observabilidad ---

// SystemMetrics contiene datos de rendimiento del nodo
type SystemMetrics struct {
	CPUPercent  float64 `json:"cpu_usage"`    // Uso de CPU simulado o real
	MemoryUsage uint64  `json:"memory_usage"` // Bytes en uso (Alloc)
	ActiveTasks int     `json:"active_tasks"` // Número de goroutines/tareas ejecutando
}

// --- Estructuras de Coordinación ---

// WorkerInfo representa un nodo trabajador registrado en el Master
type WorkerInfo struct {
	ID            string        `json:"id"`
	URL           string        `json:"url"`
	LastHeartbeat time.Time     `json:"last_heartbeat"`
	Status        string        `json:"status"`  // "UP", "DOWN"
	Metrics       SystemMetrics `json:"metrics"` // <--- NUEVO: Métricas del worker
}

// RegisterRequest es el JSON que envía el worker al iniciar
type RegisterRequest struct {
	ID   string `json:"id"`
	Port int    `json:"port"`
}

// HeartbeatRequest señal de vida con métricas
type HeartbeatRequest struct {
	ID      string        `json:"id"`
	Metrics SystemMetrics `json:"metrics"` // <--- NUEVO: Enviar métricas en cada latido
}

// --- Estructuras de Jobs y Tareas ---

// JobRequest mapea el JSON enviado por el cliente
type JobRequest struct {
	Name        string `json:"name"`
	DAG         DAG    `json:"dag"`
	Parallelism int    `json:"parallelism"`
}

type DAG struct {
	Nodes []DAGNode  `json:"nodes"`
	Edges [][]string `json:"edges"`
}

type DAGNode struct {
	ID         string `json:"id"`
	Op         string `json:"op"`
	Fn         string `json:"fn,omitempty"`
	Path       string `json:"path,omitempty"`
	Partitions int    `json:"partitions,omitempty"`
	Key        string `json:"key,omitempty"`
}

type Job struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	Graph     DAG       `json:"dag"`
	Submitted time.Time `json:"submitted_at"`
	Completed time.Time `json:"completed_at,omitempty"`
}

type Task struct {
	ID         string   `json:"id"`
	JobID      string   `json:"job_id"`
	NodeID     string   `json:"node_id"`
	Op         string   `json:"op"`
	Fn         string   `json:"fn"`
	Args       []string `json:"args"`
	InputFiles []string `json:"input_files"`
	Partition  int      `json:"partition"`
	Attempt    int      `json:"attempt"` // Para reintentos
}

type TaskResult struct {
	ID       string `json:"id"`
	JobID    string `json:"job_id"`
	NodeID   string `json:"node_id"`
	Status   string `json:"status"`
	Result   string `json:"result"`
	ErrorMsg string `json:"error_msg,omitempty"`
}

// --- Respuestas de API ---

// JobStatusResponse enriquecido con progreso y métricas
type JobStatusResponse struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Status       string            `json:"status"`
	Submitted    time.Time         `json:"submitted_at"`
	DurationSecs float64           `json:"duration_secs"`
	Progress     float64           `json:"progress_percent"` // <--- NUEVO: % de avance (0-100)
	NodeStatus   map[string]string `json:"node_status"`      // Detalle por nodo
	Failures     int               `json:"failure_count"`    // <--- NUEVO: Contador de fallos
}

// JobResultsResponse para la descarga de resultados finales
type JobResultsResponse struct {
	JobID   string            `json:"job_id"`
	Outputs map[string]string `json:"outputs"` // NodeID -> FilePath
}