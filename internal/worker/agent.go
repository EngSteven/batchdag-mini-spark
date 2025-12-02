/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: agent.go
Descripcion: Agente de comunicacion del nodo Worker con el Master.
             Maneja registro inicial, envio de heartbeats periodicos
             con metricas de sistema, y servidor HTTP para recibir tareas.
             Implementa auto-recuperacion ante desconexiones.
*/

package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"mini-spark/internal/common"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Worker representa un nodo trabajador del cluster
type Worker struct {
	ID          string // UUID unico del worker
	Port        int    // Puerto HTTP donde escucha el worker
	MasterURL   string // URL del nodo Master (http://host:port)
	OutputDir   string // Directorio para archivos temporales de salida
	ActiveTasks int32  // Contador atomico de tareas en ejecucion
}

// NewWorker - Constructor del nodo Worker
// Entrada: port - puerto HTTP, masterURL - URL del Master, outputDir - dir de salida
// Salida: puntero a instancia Worker inicializada
// Descripcion: Crea worker con UUID autogenerado, inicializa contadores a 0.
func NewWorker(port int, masterURL, outputDir string) *Worker {
	return &Worker{
		ID:        uuid.New().String(),
		Port:      port,
		MasterURL: masterURL,
		OutputDir: outputDir,
	}
}

// Start - Inicia el worker y lo conecta al cluster
// Entrada: ninguna
// Salida: ninguna (void), bloqueante en sendHeartbeat
// Descripcion: 1) Arranca servidor HTTP para recibir tareas
//  2. Se registra en el Master con retry
//  3. Inicia loop de heartbeats infinito
func (w *Worker) Start() {
	// 1. Iniciar Servidor HTTP en goroutine separada
	go func() {
		http.HandleFunc("/task", w.TaskHandler)
		addr := fmt.Sprintf(":%d", w.Port)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatalf("Fallo al iniciar worker: %v", err)
		}
	}()

	// 2. Registro en Master con retry
	for {
		if w.register() == nil {
			fmt.Println("[WORKER] Registrado en Master")
			break
		}
		time.Sleep(2 * time.Second) // Retry cada 2s si falla
	}

	// 3. Loop de Heartbeats (bloqueante)
	w.sendHeartbeat()
}

// register - Envia peticion de registro al Master
// Entrada: ninguna
// Salida: error si falla conexion o HTTP
// Descripcion: Serializa RegisterRequest con ID y puerto, lo envia
//
//	via POST a /register del Master.
func (w *Worker) register() error {
	req := common.RegisterRequest{ID: w.ID, Port: w.Port}
	data, _ := json.Marshal(req)
	resp, err := http.Post(w.MasterURL+"/register", "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

// sendHeartbeat - Loop infinito de envio de heartbeats al Master
// Entrada: ninguna
// Salida: ninguna (void), loop infinito
// Descripcion: Cada 3 segundos captura metricas del runtime Go (goroutines,
//
//	memoria, tareas activas) y las envia al Master via POST /heartbeat.
//	Permite al Master detectar workers caidos.
func (w *Worker) sendHeartbeat() {
	for {
		// Capturar metricas del runtime
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		metrics := common.SystemMetrics{
			CPUPercent:  float64(runtime.NumGoroutine()),       // Proxy: num de goroutines
			MemoryUsage: m.Alloc,                               // Bytes alocados en heap
			ActiveTasks: int(atomic.LoadInt32(&w.ActiveTasks)), // Tareas concurrentes
		}
		// Construir request de heartbeat
		req := common.HeartbeatRequest{ID: w.ID, Metrics: metrics}
		data, _ := json.Marshal(req)

		// Enviar POST al Master
		resp, err := http.Post(w.MasterURL+"/heartbeat", "application/json", bytes.NewBuffer(data))
		if err == nil {
			resp.Body.Close()
		} else {
			fmt.Printf("Error enviando heartbeat: %v\n", err)
		}
		time.Sleep(3 * time.Second) // Intervalo de heartbeat
	}
}

// TaskHandler - Handler HTTP para recibir tareas del Master
// Entrada: rw - response writer, r - request con Task JSON
// Salida: HTTP 200 OK o 400 Bad Request
// Descripcion: Deserializa Task del body, responde inmediatamente OK,
//
//	y lanza ejecucion en goroutine separada para no bloquear.
func (w *Worker) TaskHandler(rw http.ResponseWriter, r *http.Request) {
	var task common.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(rw, "Bad Request", http.StatusBadRequest)
		return
	}
	// Responder inmediatamente
	rw.WriteHeader(http.StatusOK)
	// Ejecutar tarea en background
	go w.ExecuteTask(task)
}
