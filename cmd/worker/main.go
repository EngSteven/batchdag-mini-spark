package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
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
	activeTasks int32 = 0
	
	// Configuración de Spill
	SpillThreshold = 1000 // Umbral configurable de claves en memoria
)

// ... (UDFs mapFunctions, filterFunctions, flatMapFunctions IGUAL QUE ANTES) ...
// Copia aquí las UDFs del código anterior

var mapFunctions = map[string]func(string) string{
	"to_lower": func(s string) string { return strings.ToLower(s) },
	"to_json": func(s string) string {
		parts := strings.SplitN(s, ",", 2)
		if len(parts) < 2 { return "{}" }
		return fmt.Sprintf(`{"key": "%s", "value": "%s"}`, strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
	},
}
var filterFunctions = map[string]func(string) bool{ "long_words": func(s string) bool { return len(s) > 4 } }
var flatMapFunctions = map[string]func(string) []string{
	"tokenize": func(s string) []string {
		s = strings.Map(func(r rune) rune { if strings.ContainsRune(".,;?!-", r) { return -1 }; return r }, s)
		return strings.Fields(s)
	},
}

func init() { os.MkdirAll(outputDir, 0755) }

// ... (FUNCIONES DE RED registerWorker, sendHeartbeat, taskHandler IGUAL QUE ANTES) ...
// Copia aquí registerWorker, sendHeartbeat, taskHandler del código anterior

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
		var m runtime.MemStats; runtime.ReadMemStats(&m)
		metrics := common.SystemMetrics{CPUPercent: float64(runtime.NumGoroutine()), MemoryUsage: m.Alloc, ActiveTasks: int(atomic.LoadInt32(&activeTasks))}
		req := common.HeartbeatRequest{ID: id, Metrics: metrics}
		data, _ := json.Marshal(req)
		http.Post(masterURL+"/heartbeat", "application/json", bytes.NewBuffer(data))
		time.Sleep(3 * time.Second)
	}
}

func taskHandler(w http.ResponseWriter, r *http.Request) {
	var task common.Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil { http.Error(w, "Bad Request", http.StatusBadRequest); return }
	w.WriteHeader(http.StatusOK)
	go executeTask(task)
}

// --- EJECUCIÓN (CON SPILL) ---

func executeTask(task common.Task) {
	atomic.AddInt32(&activeTasks, 1); defer atomic.AddInt32(&activeTasks, -1)
	fmt.Printf("[WORKER %d] Ejecutando %s (Op: %s)\n", *workerPort, task.NodeID, task.Op)
	outputFile := fmt.Sprintf("%s/%s_%s.txt", outputDir, task.JobID, task.NodeID)
	
	var err error
	switch task.Op {
	case "read_csv", "read_jsonl": err = opReadCSV(task.Args[0], outputFile)
	case "map": err = opMap(task.InputFiles, outputFile, task.Fn)
	case "flat_map": err = opFlatMap(task.InputFiles, outputFile, task.Fn)
	case "filter": err = opFilter(task.InputFiles, outputFile, task.Fn)
	case "reduce_by_key": 
		// USAMOS LA NUEVA VERSIÓN CON SPILL
		err = opReduceByKeyWithSpill(task.InputFiles, outputFile) 
	case "join":
		if len(task.InputFiles) >= 2 { err = opJoin(task.InputFiles[0], task.InputFiles[1], outputFile)
		} else { err = fmt.Errorf("join requiere 2 inputs") }
	default: err = fmt.Errorf("op desconocida")
	}

	status := "COMPLETED"; errorMsg := ""
	if err != nil { status = "FAILED"; errorMsg = err.Error(); fmt.Printf("Error: %v\n", err) }
	reportCompletion(task, status, outputFile, errorMsg)
}

// ... (OPERADORES STANDARD opReadCSV, opMap, opFlatMap, opFilter, opJoin IGUAL QUE ANTES) ...

func opReadCSV(in, out string) error {
	fIn, err := os.Open(in); if err!=nil {return err}; defer fIn.Close()
	fOut, err := os.Create(out); if err!=nil {return err}; defer fOut.Close()
	sc := bufio.NewScanner(fIn); wr := bufio.NewWriter(fOut)
	for sc.Scan() { wr.WriteString(sc.Text() + "\n") }; return wr.Flush()
}
func opMap(ins []string, out, fn string) error {
	f, err := os.Create(out); if err!=nil {return err}; defer f.Close(); w := bufio.NewWriter(f)
	for _, in := range ins { fIn, _ := os.Open(in); if fIn==nil {continue}; sc := bufio.NewScanner(fIn); for sc.Scan() { w.WriteString(mapFunctions[fn](sc.Text()) + "\n") }; fIn.Close() }; return w.Flush()
}
func opFlatMap(ins []string, out, fn string) error {
	f, err := os.Create(out); if err!=nil {return err}; defer f.Close(); w := bufio.NewWriter(f)
	for _, in := range ins { fIn, _ := os.Open(in); if fIn==nil {continue}; sc := bufio.NewScanner(fIn); for sc.Scan() { for _, s := range flatMapFunctions[fn](sc.Text()) { w.WriteString(s + "\n") } }; fIn.Close() }; return w.Flush()
}
func opFilter(ins []string, out, fn string) error {
	f, err := os.Create(out); if err!=nil {return err}; defer f.Close(); w := bufio.NewWriter(f)
	for _, in := range ins { fIn, _ := os.Open(in); if fIn==nil {continue}; sc := bufio.NewScanner(fIn); for sc.Scan() { if filterFunctions[fn](sc.Text()) { w.WriteString(sc.Text() + "\n") } }; fIn.Close() }; return w.Flush()
}
func opJoin(left, right, out string) error {
	lMap := make(map[string]string); fL, _ := os.Open(left); scL := bufio.NewScanner(fL)
	for scL.Scan() { p := strings.SplitN(scL.Text(), ",", 2); if len(p)==2 {lMap[p[0]]=p[1]} }; fL.Close()
	fR, _ := os.Open(right); defer fR.Close(); scR := bufio.NewScanner(fR); fOut, _ := os.Create(out); defer fOut.Close(); w := bufio.NewWriter(fOut)
	for scR.Scan() { p := strings.SplitN(scR.Text(), ",", 2); if len(p)==2 { if vL, ok := lMap[p[0]]; ok { w.WriteString(fmt.Sprintf("%s, %s, %s\n", p[0], vL, p[1])) } } }; return w.Flush()
}

// --- NUEVO: REDUCE CON SPILL ---

func opReduceByKeyWithSpill(inputs []string, outputFile string) error {
	counts := make(map[string]int)
	var spillFiles []string

	// Fase 1: Lectura y Spill parcial
	for _, in := range inputs {
		file, err := os.Open(in)
		if err != nil { continue }
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			counts[scanner.Text()]++

			// CHEQUEO DE MEMORIA (Umbral)
			if len(counts) >= SpillThreshold {
				spillName := fmt.Sprintf("%s_spill_%d.tmp", outputFile, len(spillFiles))
				if err := dumpSpill(counts, spillName); err != nil { return err }
				spillFiles = append(spillFiles, spillName)
				counts = make(map[string]int) // Limpiar memoria
				fmt.Printf("   -> Spill to disk: %s\n", spillName)
			}
		}
		file.Close()
	}

	// Fase 2: Merge (Memoria + Archivos Spill)
	// Cargamos los spills secuencialmente (simplificación vs merge sort real)
	for _, spill := range spillFiles {
		file, err := os.Open(spill)
		if err != nil { continue }
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			// Formato spill: "key, count"
			parts := strings.SplitN(scanner.Text(), ",", 2)
			if len(parts) == 2 {
				var val int
				fmt.Sscanf(parts[1], "%d", &val)
				counts[parts[0]] += val
			}
		}
		file.Close()
		os.Remove(spill) // Limpiar tmp
	}

	// Fase 3: Escritura Final
	f, err := os.Create(outputFile)
	if err != nil { return err }
	defer f.Close()
	w := bufio.NewWriter(f)
	for k, v := range counts {
		w.WriteString(fmt.Sprintf("%s, %d\n", k, v))
	}
	return w.Flush()
}

func dumpSpill(data map[string]int, filename string) error {
	f, err := os.Create(filename)
	if err != nil { return err }
	defer f.Close()
	w := bufio.NewWriter(f)
	for k, v := range data {
		w.WriteString(fmt.Sprintf("%s,%d\n", k, v))
	}
	return w.Flush()
}

func reportCompletion(task common.Task, status, resPath, errorMsg string) {
	res := common.TaskResult{ID: task.ID, JobID: task.JobID, NodeID: task.NodeID, Status: status, Result: resPath, ErrorMsg: errorMsg}
	data, _ := json.Marshal(res)
	for i := 0; i < 3; i++ {
		if _, err := http.Post(masterURL+"/task/complete", "application/json", bytes.NewBuffer(data)); err == nil { return }
		time.Sleep(1 * time.Second)
	}
}

func main() {
	flag.Parse()
	id := uuid.New().String()
	go func() { http.HandleFunc("/task", taskHandler); http.ListenAndServe(fmt.Sprintf(":%d", *workerPort), nil) }()
	for { if registerWorker(id)==nil { break }; time.Sleep(2*time.Second) }
	sendHeartbeat(id)
}