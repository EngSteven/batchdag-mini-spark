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
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	masterURL  = "http://localhost:8080"
	workerPort = flag.Int("port", 9001, "Puerto del worker")
	outputDir  = "/tmp/mini-spark" // Directorio compartido simulado
)

// --- REGISTRO DE FUNCIONES (UDFs) ---
// En un sistema real (Spark/Flink), el usuario envía el código serializado.
// Aquí hardcodeamos las funciones soportadas.

var mapFunctions = map[string]func(string) string{
	"to_lower": func(s string) string { return strings.ToLower(s) },
	"identity": func(s string) string { return s },
}

var flatMapFunctions = map[string]func(string) []string{
	"tokenize": func(s string) []string {
		// Normalizar y separar por espacios
		s = strings.Map(func(r rune) rune {
			if strings.ContainsRune(".,;?!-", r) {
				return -1 // Eliminar puntuación básica
			}
			return r
		}, s)
		return strings.Fields(s)
	},
}

// --- INFRAESTRUCTURA ---

func init() {
	// Asegurar que existe el directorio temporal
	os.MkdirAll(outputDir, 0755)
}

func registerWorker(id string) error {
	req := common.RegisterRequest{ID: id, Port: *workerPort}
	data, _ := json.Marshal(req)
	resp, err := http.Post(masterURL+"/register", "application/json", bytes.NewBuffer(data))
	if err != nil { return err }
	defer resp.Body.Close()
	return nil
}

func sendHeartbeat(id string) {
	req := common.HeartbeatRequest{ID: id}
	data, _ := json.Marshal(req)
	for {
		http.Post(masterURL+"/heartbeat", "application/json", bytes.NewBuffer(data))
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

// --- MOTOR DE EJECUCIÓN ---

func executeTask(task common.Task) {
	fmt.Printf("[WORKER %d] Ejecutando %s (Op: %s)...\n", *workerPort, task.NodeID, task.Op)

	// Definir archivo de salida
	outputFile := fmt.Sprintf("%s/%s_%s.txt", outputDir, task.JobID, task.NodeID)
	
	var err error
	
	// Switch de Operadores
	switch task.Op {
	case "read_csv":
		// En Batch DAG, read_csv lee un archivo origen y escribe en formato interno
		err = opReadCSV(task.Args[0], outputFile)
	case "map":
		err = opMap(task.InputFiles, outputFile, task.Fn)
	case "flat_map":
		err = opFlatMap(task.InputFiles, outputFile, task.Fn)
	case "reduce_by_key":
		// Dejaremos esto como "dummy" por ahora, solo copia datos para no romper el flujo
		err = opReduceByKey(task.InputFiles, outputFile, task.Fn)
	default:
		err = fmt.Errorf("operación no soportada: %s", task.Op)
	}

	status := "COMPLETED"
	if err != nil {
		fmt.Printf("[WORKER %d] Error en tarea %s: %v\n", *workerPort, task.NodeID, err)
		status = "FAILED"
	} else {
		fmt.Printf("[WORKER %d] Tarea %s completada. Output: %s\n", *workerPort, task.NodeID, outputFile)
	}

	reportCompletion(task, status, outputFile)
}

// --- OPERADORES ---

func opReadCSV(inputPath string, outputPath string) error {
	// Leer archivo fuente
	inFile, err := os.Open(inputPath)
	if err != nil { return err }
	defer inFile.Close()

	// Crear archivo salida
	outFile, err := os.Create(outputPath)
	if err != nil { return err }
	defer outFile.Close()

	scanner := bufio.NewScanner(inFile)
	writer := bufio.NewWriter(outFile)

	for scanner.Scan() {
		line := scanner.Text()
		// Aquí podríamos parsear CSV, pero por simplicidad pasamos la línea cruda
		writer.WriteString(line + "\n")
	}
	return writer.Flush()
}

func opMap(inputFiles []string, outputPath string, fnName string) error {
	fn, ok := mapFunctions[fnName]
	if !ok { return fmt.Errorf("función map no encontrada: %s", fnName) }

	outFile, err := os.Create(outputPath)
	if err != nil { return err }
	defer outFile.Close()
	writer := bufio.NewWriter(outFile)

	for _, inputFile := range inputFiles {
		inFile, err := os.Open(inputFile)
		if err != nil { continue } // Si falla un input, seguimos (naive)
		
		scanner := bufio.NewScanner(inFile)
		for scanner.Scan() {
			res := fn(scanner.Text())
			writer.WriteString(res + "\n")
		}
		inFile.Close()
	}
	return writer.Flush()
}

func opFlatMap(inputFiles []string, outputPath string, fnName string) error {
	fn, ok := flatMapFunctions[fnName]
	if !ok { return fmt.Errorf("función flat_map no encontrada: %s", fnName) }

	outFile, err := os.Create(outputPath)
	if err != nil { return err }
	defer outFile.Close()
	writer := bufio.NewWriter(outFile)

	for _, inputFile := range inputFiles {
		inFile, err := os.Open(inputFile)
		if err != nil { continue }
		
		scanner := bufio.NewScanner(inFile)
		for scanner.Scan() {
			results := fn(scanner.Text()) // Retorna array de strings
			for _, item := range results {
				writer.WriteString(item + "\n")
			}
		}
		inFile.Close()
	}
	return writer.Flush()
}

func opDummy(inputFiles []string, outputPath string) error {
    // Simula éxito creando un archivo vacío o copiando
    f, err := os.Create(outputPath)
    if err != nil { return err }
    f.WriteString("resultado_simulado\n")
    f.Close()
    return nil
}

func opReduceByKey(inputFiles []string, outputPath string, fnName string) error {
	// 1. Estructura para agregación en memoria (Key -> Value)
	// En un caso real, esto debería hacer "spill to disk" si la memoria se llena.
	counts := make(map[string]int)

	fmt.Printf("   -> Iniciando Reduce sobre %d archivos de entrada\n", len(inputFiles))

	// 2. Leer TODOS los archivos de entrada (Shuffle Read)
	for _, inputFile := range inputFiles {
		file, err := os.Open(inputFile)
		if err != nil {
			fmt.Printf("   -> Error leyendo input %s: %v\n", inputFile, err)
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			key := scanner.Text()
			// En WordCount, cada línea es una ocurrencia (valor 1)
			if fnName == "sum" {
				counts[key]++
			}
		}
		file.Close()
	}

	// 3. Escribir resultados finales
	outFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outFile.Close()
	writer := bufio.NewWriter(outFile)

	for key, count := range counts {
		// Formato de salida: "palabra, cantidad"
		line := fmt.Sprintf("%s, %d\n", key, count)
		writer.WriteString(line)
	}

	return writer.Flush()
}

// --- REPORTE ---

func reportCompletion(task common.Task, status, resultPath string) {
	res := common.TaskResult{
		ID:     task.ID,
		JobID:  task.JobID,
		NodeID: task.NodeID,
		Status: status,
		Result: resultPath,
	}
	data, _ := json.Marshal(res)
	http.Post(masterURL+"/task/complete", "application/json", bytes.NewBuffer(data))
}

func main() {
	flag.Parse()
	id := uuid.New().String()
	
	go func() {
		http.HandleFunc("/task", taskHandler)
		http.ListenAndServe(fmt.Sprintf(":%d", *workerPort), nil)
	}()

	for {
		if registerWorker(id) == nil { break }
		time.Sleep(2 * time.Second)
	}
	sendHeartbeat(id)
}