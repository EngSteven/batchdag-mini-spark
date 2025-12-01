package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"mini-spark/internal/common"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var (
	masterURL   = "http://localhost:8080"
	workerPort  = flag.Int("port", 9001, "Puerto del worker")
	outputDir   = "/tmp/mini-spark"
	activeTasks int32 = 0 // Contador atómico de tareas activas
)

// --- UDFs (Funciones de Usuario) ---

var mapFunctions = map[string]func(string) string{
	"to_lower": func(s string) string { return strings.ToLower(s) },
}

var filterFunctions = map[string]func(string) bool{
	"long_words": func(s string) bool { return len(s) > 4 },
}

var flatMapFunctions = map[string]func(string) []string{
	"tokenize": func(s string) []string {
		s = strings.Map(func(r rune) rune {
			if strings.ContainsRune(".,;?!-", r) { return -1 }
			return r
		}, s)
		return strings.Fields(s)
	},
}

// --- INIT ---

func init() {
	os.MkdirAll(outputDir, 0755)
}

// --- NETWORKING ---

func registerWorker(id string) error {
	req := common.RegisterRequest{ID: id, Port: *workerPort}
	data, _ := json.Marshal(req)
	resp, err := http.Post(masterURL+"/register", "application/json", bytes.NewBuffer(data))
	if err != nil { return err }
	defer resp.Body.Close()
	return nil
}

func sendHeartbeat(id string) {
	for {
		// 1. Recolectar métricas del sistema
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		metrics := common.SystemMetrics{
			// Usamos el número de gorutinas como proxy simple de carga de CPU
			CPUPercent: float64(runtime.NumGoroutine()),
			// Memoria asignada en bytes
			MemoryUsage: m.Alloc,
			// Tareas de mini-spark ejecutándose
			ActiveTasks: int(atomic.LoadInt32(&activeTasks)),
		}

		// 2. Enviar Heartbeat con métricas
		req := common.HeartbeatRequest{ID: id, Metrics: metrics}
		data, _ := json.Marshal(req)
		
		resp, err := http.Post(masterURL+"/heartbeat", "application/json", bytes.NewBuffer(data))
		if err != nil {
			fmt.Printf("[WORKER %d] Error enviando heartbeat: %v\n", *workerPort, err)
		} else {
			resp.Body.Close()
		}
		
		time.Sleep(3 * time.Second)
	}
}

func taskHandler(w http.ResponseWriter, r *http.Request) {
	var task common.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	go executeTask(task)
}

// --- EJECUCIÓN PRINCIPAL ---

func executeTask(task common.Task) {
	// Registrar inicio de tarea
	atomic.AddInt32(&activeTasks, 1)
	defer atomic.AddInt32(&activeTasks, -1) // Asegurar decremento al salir

	fmt.Printf("[WORKER %d] Ejecutando %s (Op: %s, Intento: %d)\n", *workerPort, task.NodeID, task.Op, task.Attempt)
	outputFile := fmt.Sprintf("%s/%s_%s.txt", outputDir, task.JobID, task.NodeID)
	
	var err error
	switch task.Op {
	case "read_csv":
		err = opReadCSV(task.Args[0], outputFile)
	case "map":
		err = opMap(task.InputFiles, outputFile, task.Fn)
	case "flat_map":
		err = opFlatMap(task.InputFiles, outputFile, task.Fn)
	case "filter":
		err = opFilter(task.InputFiles, outputFile, task.Fn)
	case "reduce_by_key":
		err = opReduceByKey(task.InputFiles, outputFile, task.Fn)
	case "join":
		if len(task.InputFiles) >= 2 {
			err = opJoin(task.InputFiles[0], task.InputFiles[1], outputFile)
		} else {
			err = fmt.Errorf("join requiere al menos 2 inputs")
		}
	default:
		err = fmt.Errorf("operación desconocida: %s", task.Op)
	}

	status := "COMPLETED"
	errorMsg := ""
	if err != nil {
		fmt.Printf("[WORKER %d] ERROR en %s: %v\n", *workerPort, task.NodeID, err)
		status = "FAILED"
		errorMsg = err.Error()
	} else {
		fmt.Printf("[WORKER %d] Completado %s\n", *workerPort, task.NodeID)
	}

	reportCompletion(task, status, outputFile, errorMsg)
}

// --- OPERADORES ---

func opReadCSV(inputPath, outputPath string) error {
	inFile, err := os.Open(inputPath)
	if err != nil { return err }
	defer inFile.Close()
	outFile, err := os.Create(outputPath)
	if err != nil { return err }
	defer outFile.Close()
	scanner := bufio.NewScanner(inFile)
	writer := bufio.NewWriter(outFile)
	for scanner.Scan() {
		writer.WriteString(scanner.Text() + "\n")
	}
	return writer.Flush()
}

func opMap(inputs []string, output string, fnName string) error {
	fn, ok := mapFunctions[fnName]
	if !ok { return fmt.Errorf("fn map no encontrada") }
	
	f, err := os.Create(output)
	if err != nil { return err }
	defer f.Close()
	w := bufio.NewWriter(f)

	for _, in := range inputs {
		file, _ := os.Open(in) // Ignora error si un input falta (best-effort)
		if file == nil { continue }
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			w.WriteString(fn(scanner.Text()) + "\n")
		}
		file.Close()
	}
	return w.Flush()
}

func opFlatMap(inputs []string, output string, fnName string) error {
	fn, ok := flatMapFunctions[fnName]
	if !ok { return fmt.Errorf("fn flat_map no encontrada") }

	f, err := os.Create(output)
	if err != nil { return err }
	defer f.Close()
	w := bufio.NewWriter(f)

	for _, in := range inputs {
		file, _ := os.Open(in)
		if file == nil { continue }
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			for _, item := range fn(scanner.Text()) {
				w.WriteString(item + "\n")
			}
		}
		file.Close()
	}
	return w.Flush()
}

func opFilter(inputs []string, output string, fnName string) error {
	fn, ok := filterFunctions[fnName]
	if !ok { return fmt.Errorf("fn filter no encontrada") }

	f, err := os.Create(output)
	if err != nil { return err }
	defer f.Close()
	w := bufio.NewWriter(f)

	for _, in := range inputs {
		file, _ := os.Open(in)
		if file == nil { continue }
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			if fn(line) {
				w.WriteString(line + "\n")
			}
		}
		file.Close()
	}
	return w.Flush()
}

func opReduceByKey(inputs []string, output string, fnName string) error {
	counts := make(map[string]int)
	for _, in := range inputs {
		file, err := os.Open(in)
		if err != nil { continue }
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			counts[scanner.Text()]++
		}
		file.Close()
	}
	f, err := os.Create(output)
	if err != nil { return err }
	defer f.Close()
	w := bufio.NewWriter(f)
	for k, v := range counts {
		w.WriteString(fmt.Sprintf("%s, %d\n", k, v))
	}
	return w.Flush()
}

func opJoin(leftFile, rightFile, output string) error {
	leftMap := make(map[string]string)
	
	lFile, err := os.Open(leftFile)
	if err != nil { return err }
	lScanner := bufio.NewScanner(lFile)
	for lScanner.Scan() {
		parts := strings.SplitN(lScanner.Text(), ",", 2)
		if len(parts) == 2 {
			leftMap[parts[0]] = parts[1]
		}
	}
	lFile.Close()

	rFile, err := os.Open(rightFile)
	if err != nil { return err }
	defer rFile.Close()
	
	outFile, err := os.Create(output)
	if err != nil { return err }
	defer outFile.Close()
	w := bufio.NewWriter(outFile)

	rScanner := bufio.NewScanner(rFile)
	for rScanner.Scan() {
		parts := strings.SplitN(rScanner.Text(), ",", 2)
		if len(parts) == 2 {
			key := parts[0]
			valRight := parts[1]
			if valLeft, ok := leftMap[key]; ok {
				w.WriteString(fmt.Sprintf("%s, %s, %s\n", key, valLeft, valRight))
			}
		}
	}
	return w.Flush()
}

// --- REPORTE ---

func reportCompletion(task common.Task, status, resultPath, errorMsg string) {
	res := common.TaskResult{
		ID:       task.ID,
		JobID:    task.JobID,
		NodeID:   task.NodeID,
		Status:   status,
		Result:   resultPath,
		ErrorMsg: errorMsg,
	}
	data, _ := json.Marshal(res)
	
	for i := 0; i < 3; i++ {
		resp, err := http.Post(masterURL+"/task/complete", "application/json", bytes.NewBuffer(data))
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(1 * time.Second)
	}
	fmt.Printf("[WORKER %d] ERROR REPORTANDO ESTADO\n", *workerPort)
}

func main() {
	flag.Parse()
	id := uuid.New().String()
	
	// Iniciar servidor worker
	go func() {
		http.HandleFunc("/task", taskHandler)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", *workerPort), nil); err != nil {
			log.Fatalf("Fallo al iniciar worker: %v", err)
		}
	}()

	// Registro con reintentos
	for {
		if registerWorker(id) == nil { break }
		time.Sleep(2 * time.Second)
	}
	
	// Loop de heartbeats (bloqueante)
	sendHeartbeat(id)
}