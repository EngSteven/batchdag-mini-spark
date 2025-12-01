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

type Worker struct {
	ID          string
	Port        int
	MasterURL   string
	OutputDir   string
	ActiveTasks int32
}

func NewWorker(port int, masterURL, outputDir string) *Worker {
	return &Worker{
		ID:        uuid.New().String(),
		Port:      port,
		MasterURL: masterURL,
		OutputDir: outputDir,
	}
}

func (w *Worker) Start() {
	// 1. Iniciar Servidor HTTP
	go func() {
		http.HandleFunc("/task", w.TaskHandler)
		addr := fmt.Sprintf(":%d", w.Port)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatalf("Fallo al iniciar worker: %v", err)
		}
	}()

	// 2. Registro
	for {
		if w.register() == nil {
			fmt.Println("[WORKER] Registrado en Master")
			break
		}
		time.Sleep(2 * time.Second)
	}

	// 3. Heartbeats
	w.sendHeartbeat()
}

func (w *Worker) register() error {
	req := common.RegisterRequest{ID: w.ID, Port: w.Port}
	data, _ := json.Marshal(req)
	resp, err := http.Post(w.MasterURL+"/register", "application/json", bytes.NewBuffer(data))
	if err != nil { return err }
	defer resp.Body.Close()
	return nil
}

func (w *Worker) sendHeartbeat() {
	for {
		var m runtime.MemStats; runtime.ReadMemStats(&m)
		metrics := common.SystemMetrics{
			CPUPercent: float64(runtime.NumGoroutine()),
			MemoryUsage: m.Alloc,
			ActiveTasks: int(atomic.LoadInt32(&w.ActiveTasks)),
		}
		req := common.HeartbeatRequest{ID: w.ID, Metrics: metrics}
		data, _ := json.Marshal(req)
		
		resp, err := http.Post(w.MasterURL+"/heartbeat", "application/json", bytes.NewBuffer(data))
		if err == nil { resp.Body.Close() } else {
			fmt.Printf("Error enviando heartbeat: %v\n", err)
		}
		time.Sleep(3 * time.Second)
	}
}

func (w *Worker) TaskHandler(rw http.ResponseWriter, r *http.Request) {
	var task common.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(rw, "Bad Request", http.StatusBadRequest); return
	}
	rw.WriteHeader(http.StatusOK)
	go w.ExecuteTask(task)
}