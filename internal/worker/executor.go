package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"bufio"
	"mini-spark/internal/common"
	"mini-spark/internal/operators"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"net/http"
)

var SpillThreshold = 1000

func (w *Worker) ExecuteTask(task common.Task) {
	atomic.AddInt32(&w.ActiveTasks, 1)
	defer atomic.AddInt32(&w.ActiveTasks, -1)

	fmt.Printf("[WORKER %d] Ejecutando %s (Op: %s)\n", w.Port, task.NodeID, task.Op)
	outputFile := fmt.Sprintf("%s/%s_%s.txt", w.OutputDir, task.JobID, task.NodeID)
	
	var err error
	switch task.Op {
	case "read_csv", "read_jsonl":
		err = operators.ReadCSV(task.Args[0], outputFile)
	case "map":
		err = operators.Map(task.InputFiles, outputFile, task.Fn)
	case "flat_map":
		err = operators.FlatMap(task.InputFiles, outputFile, task.Fn)
	case "reduce_by_key":
		// Mantenemos logica de spill local aqui por ahora, o la movemos a operators si es pura
		// Para simplificar, implementaré la version con spill aquí mismo usando el patrón del código anterior
		// pero encapsulado como método privado o usando helpers.
		err = opReduceByKeyWithSpill(task.InputFiles, outputFile)
	case "join":
		if len(task.InputFiles) >= 2 {
			err = operators.Join(task.InputFiles[0], task.InputFiles[1], outputFile)
		} else { err = fmt.Errorf("join requiere 2 inputs") }
	default:
		err = fmt.Errorf("operación desconocida: %s", task.Op)
	}

	status := "COMPLETED"; errorMsg := ""
	if err != nil {
		status = "FAILED"; errorMsg = err.Error(); fmt.Printf("Error: %v\n", err)
	}
	w.reportCompletion(task, status, outputFile, errorMsg)
}

func (w *Worker) reportCompletion(task common.Task, status, resPath, err string) {
	res := common.TaskResult{ID: task.ID, JobID: task.JobID, NodeID: task.NodeID, Status: status, Result: resPath, ErrorMsg: err}
	data, _ := json.Marshal(res)
	for i := 0; i < 3; i++ {
		if _, e := http.Post(w.MasterURL+"/task/complete", "application/json", bytes.NewBuffer(data)); e == nil { return }
		time.Sleep(1 * time.Second)
	}
	fmt.Printf("ERROR REPORTANDO TAREA\n")
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