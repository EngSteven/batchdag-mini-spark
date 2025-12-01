package common

import "time"

// WorkerInfo representa un nodo trabajador registrado en el Master
type WorkerInfo struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"` // Dirección IP:Puerto del worker para recibir tareas
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Status    string    `json:"status"` // "UP", "DOWN"
}

// RegisterRequest es el JSON que envía el worker al iniciar
type RegisterRequest struct {
	ID   string `json:"id"`
	Port int    `json:"port"` // Puerto donde el worker escucha tareas
}

// HeartbeatRequest señal de vida
type HeartbeatRequest struct {
	ID string `json:"id"`
}

// --- Estructuras para Definición de Jobs (Batch) ---

// JobRequest mapea el JSON enviado por el cliente
type JobRequest struct {
	Name        string `json:"name"`
	DAG         DAG    `json:"dag"`
	Parallelism int    `json:"parallelism"`
}


// DAG representa un grafo acíclico dirigido de operaciones a ejecutar
type DAG struct {
	Nodes []DAGNode   `json:"nodes"`
	Edges [][]string  `json:"edges"` // Lista de pares ["origen", "destino"]
}

// DAGNode representa una operación en el DAG
type DAGNode struct {
	ID         string `json:"id"`
	Op         string `json:"op"` // "read_csv", "map", "reduce_by_key", etc.
	Fn         string `json:"fn,omitempty"` // Nombre de la función a ejecutar
	Path       string `json:"path,omitempty"` // Para inputs
	Partitions int    `json:"partitions,omitempty"`
	Key        string `json:"key,omitempty"` // Para reduce/join
}

// Job almacena el estado completo de un trabajo en el Master
type Job struct {
	ID        string      `json:"id"`
	Name      string      `json:"name"`
	Status    string      `json:"status"` // "ACCEPTED", "RUNNING", "COMPLETED", "FAILED"
	Graph     DAG         `json:"dag"`
	Submitted time.Time   `json:"submitted_at"`
}


// Task representa una unidad de trabajo enviada a un worker
type Task struct {
	ID        string   `json:"id"`
	JobID     string   `json:"job_id"`
	NodeID    string   `json:"node_id"` // ID del nodo en el DAG (ej: "read")
	Op        string   `json:"op"`      // Operación: "read_csv", "map", etc.
	Fn        string   `json:"fn"`      // Función a aplicar
	Args      []string `json:"args"`    // Argumentos extra (ej: path archivo)
	InputFiles []string `json:"input_files"`
	Partition int      `json:"partition"`
}

// TaskResult reporte del worker al master
type TaskResult struct {
	ID        string `json:"id"`        // ID de la Tarea
	JobID     string `json:"job_id"`
	NodeID    string `json:"node_id"`
	Status    string `json:"status"`    // "COMPLETED", "FAILED"
	Result    string `json:"result"`    // (Opcional) Path de salida o mensaje
}