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
	// Leemos variable de entorno para Docker o usamos default para local
	masterURL      = getEnv("MASTER_URL", "http://localhost:8080")
	workerPort     = flag.Int("port", 9001, "Puerto del worker")
	outputDir      = "/tmp/mini-spark"
	activeTasks    int32 = 0
	SpillThreshold = 1000 // Umbral de claves en memoria antes de volcar a disco
)

// Helper para leer variables de entorno
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// --- UDFs (Funciones de Usuario) ---

var mapFunctions = map[string]func(string) string{
	"to_lower": func(s string) string { return strings.ToLower(s) },
	"to_json": func(s string) string {
		parts := strings.SplitN(s, ",", 2)
		if len(parts) < 2 {
			return "{}"
		}
		return fmt.Sprintf(`{"key": "%s", "value": "%s"}`, strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
	},
}

var filterFunctions = map[string]func(string) bool{
	"long_words": func(s string) bool { return len(s) > 4 },
}

var flatMapFunctions = map[string]func(string) []string{
	"tokenize": func(s string) []string {
		s = strings.Map(func(r rune) rune {
			if strings.ContainsRune(".,;?!-", r) {
				return -1
			}
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
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func sendHeartbeat(id string) {
	for {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		metrics := common.SystemMetrics{
			CPUPercent:  float64(runtime.NumGoroutine()),
			MemoryUsage: m.Alloc,
			ActiveTasks: int(atomic.LoadInt32(&activeTasks)),
		}

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
	atomic.AddInt32(&activeTasks, 1)
	defer atomic.AddInt32(&activeTasks, -1)

	fmt.Printf("[WORKER %d] Ejecutando %s (Op: %s)\n", *workerPort, task.NodeID, task.Op)
	outputFile := fmt.Sprintf("%s/%s_%s.txt", outputDir, task.JobID, task.NodeID)

	var err error
	switch task.Op {
	case "read_csv", "read_jsonl":
		err = opReadCSV(task.Args[0], outputFile)
	case "map":
		err = opMap(task.InputFiles, outputFile, task.Fn)
	case "flat_map":
		err = opFlatMap(task.InputFiles, outputFile, task.Fn)
	case "filter":
		err = opFilter(task.InputFiles, outputFile, task.Fn)
	case "reduce_by_key":
		// Usamos la versión con Spill para cumplir rúbrica de Memoria
		err = opReduceByKeyWithSpill(task.InputFiles, outputFile)
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

// --- OPERADORES BÁSICOS ---

func opReadCSV(inputPath, outputPath string) error {
	inFile, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer inFile.Close()
	outFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
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
	if !ok {
		return fmt.Errorf("fn map no encontrada: %s", fnName)
	}

	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	for _, in := range inputs {
		file, _ := os.Open(in)
		if file == nil {
			continue
		}
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
	if !ok {
		return fmt.Errorf("fn flat_map no encontrada")
	}

	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	for _, in := range inputs {
		file, _ := os.Open(in)
		if file == nil {
			continue
		}
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
	if !ok {
		return fmt.Errorf("fn filter no encontrada")
	}

	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	for _, in := range inputs {
		file, _ := os.Open(in)
		if file == nil {
			continue
		}
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

func opJoin(leftFile, rightFile, output string) error {
	leftMap := make(map[string]string)
	lFile, err := os.Open(leftFile)
	if err != nil {
		return err
	}
	lScanner := bufio.NewScanner(lFile)
	for lScanner.Scan() {
		parts := strings.SplitN(lScanner.Text(), ",", 2)
		if len(parts) == 2 {
			leftMap[parts[0]] = parts[1]
		}
	}
	lFile.Close()

	rFile, err := os.Open(rightFile)
	if err != nil {
		return err
	}
	defer rFile.Close()

	outFile, err := os.Create(output)
	if err != nil {
		return err
	}
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

// --- REDUCE CON SPILL (MEMORIA + DISCO) ---

func opReduceByKeyWithSpill(inputs []string, outputFile string) error {
	counts := make(map[string]int)
	var spillFiles []string

	// Fase 1: Lectura y Spill parcial
	for _, in := range inputs {
		file, err := os.Open(in)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			counts[scanner.Text()]++

			// CHEQUEO DE MEMORIA
			if len(counts) >= SpillThreshold {
				spillName := fmt.Sprintf("%s_spill_%d.tmp", outputFile, len(spillFiles))
				if err := dumpSpill(counts, spillName); err != nil {
					return err
				}
				spillFiles = append(spillFiles, spillName)
				counts = make(map[string]int) // Liberar memoria
				fmt.Printf("   -> Spill to disk: %s\n", spillName)
			}
		}
		file.Close()
	}

	// Fase 2: Merge (Memoria + Archivos Spill)
	for _, spill := range spillFiles {
		file, err := os.Open(spill)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), ",", 2)
			if len(parts) == 2 {
				var val int
				fmt.Sscanf(parts[1], "%d", &val)
				counts[parts[0]] += val
			}
		}
		file.Close()
		os.Remove(spill) // Borrar temporal
	}

	// Fase 3: Escritura Final
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for k, v := range counts {
		w.WriteString(fmt.Sprintf("%s, %d\n", k, v))
	}
	return w.Flush()
}

func dumpSpill(data map[string]int, filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for k, v := range data {
		w.WriteString(fmt.Sprintf("%s,%d\n", k, v))
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
	fmt.Printf("[WORKER %d] ERROR CRÍTICO REPORTANDO ESTADO\n", *workerPort)
}

func main() {
	flag.Parse()
	id := uuid.New().String()

	// Iniciar servidor worker
	go func() {
		http.HandleFunc("/task", taskHandler)
		addr := fmt.Sprintf(":%d", *workerPort)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatalf("Fallo al iniciar worker: %v", err)
		}
	}()

	// Registro con reintentos
	for {
		if registerWorker(id) == nil {
			fmt.Println("[WORKER] Registrado en Master")
			break
		}
		time.Sleep(2 * time.Second)
	}

	// Loop de heartbeats (bloqueante)
	sendHeartbeat(id)
}