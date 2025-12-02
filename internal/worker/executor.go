/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: executor.go
Descripcion: Motor de ejecucion de tareas del Worker.
             Procesa operadores (map, reduce, join) usando paquete operators,
             implementa reduce con spill a disco para datasets grandes,
             y reporta resultados/errores al Master con reintentos.
*/

package worker

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"mini-spark/internal/common"
	"mini-spark/internal/operators"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// SpillThreshold - Limite de claves en memoria antes de spill a disco
// Usado por reduce_by_key para evitar OOM en datasets grandes
var SpillThreshold = 1000

// ExecuteTask - Ejecuta una tarea asignada por el Master
// Entrada: task - objeto Task con operacion, inputs y parametros
// Salida: ninguna (void), reporta resultado al Master
// Descripcion: Incrementa contador de tareas, ejecuta operador correspondiente,
//
//	captura errores, y reporta completado/fallido al Master.
//	Soporta: read_csv, map, flat_map, reduce_by_key, join.
func (w *Worker) ExecuteTask(task common.Task) {
	// Incrementar contador atomico de tareas activas
	atomic.AddInt32(&w.ActiveTasks, 1)
	defer atomic.AddInt32(&w.ActiveTasks, -1)

	fmt.Printf("[WORKER %d] Ejecutando %s (Op: %s)\n", w.Port, task.NodeID, task.Op)
	// Construir path de archivo de salida
	outputFile := fmt.Sprintf("%s/%s_%s.txt", w.OutputDir, task.JobID, task.NodeID)

	var err error
	// Ejecutar operador segun tipo de tarea
	switch task.Op {
	case "read_csv", "read_jsonl":
		err = operators.ReadCSV(task.Args[0], outputFile)
	case "map":
		err = operators.Map(task.InputFiles, outputFile, task.Fn)
	case "flat_map":
		err = operators.FlatMap(task.InputFiles, outputFile, task.Fn)
	case "reduce_by_key":
		// Usar implementacion con spill para manejar datasets grandes
		err = opReduceByKeyWithSpill(task.InputFiles, outputFile)
	case "join":
		if len(task.InputFiles) >= 2 {
			err = operators.Join(task.InputFiles[0], task.InputFiles[1], outputFile)
		} else {
			err = fmt.Errorf("join requiere 2 inputs")
		}
	default:
		err = fmt.Errorf("operación desconocida: %s", task.Op)
	}

	// Determinar estado de la tarea
	status := "COMPLETED"
	errorMsg := ""
	if err != nil {
		status = "FAILED"
		errorMsg = err.Error()
		fmt.Printf("Error: %v\n", err)
	}
	// Reportar resultado al Master
	w.reportCompletion(task, status, outputFile, errorMsg)
}

// reportCompletion - Envia resultado de tarea al Master
// Entrada: task - tarea ejecutada, status - COMPLETED|FAILED, resPath - archivo salida, err - error
// Salida: ninguna (void)
// Descripcion: Construye TaskResult y lo envia via POST a /task/complete.
//
//	Reintenta hasta 3 veces si falla la conexion.
func (w *Worker) reportCompletion(task common.Task, status, resPath, err string) {
	res := common.TaskResult{ID: task.ID, JobID: task.JobID, NodeID: task.NodeID, Status: status, Result: resPath, ErrorMsg: err}
	data, _ := json.Marshal(res)
	// Reintentar hasta 3 veces
	for i := 0; i < 3; i++ {
		if _, e := http.Post(w.MasterURL+"/task/complete", "application/json", bytes.NewBuffer(data)); e == nil {
			return
		}
		time.Sleep(1 * time.Second)
	}
	fmt.Printf("ERROR REPORTANDO TAREA\n")
}

// --- REDUCE CON SPILL (MEMORIA + DISCO) ---

// opReduceByKeyWithSpill - Implementa reduce con gestion de memoria
// Entrada: inputs - slice de archivos con claves, outputFile - destino
// Salida: error si falla I/O
// Descripcion: Reduce grande que no cabe en memoria:
//
//	Fase 1: Lee inputs, acumula en mapa, hace spill a disco si supera threshold
//	Fase 2: Merge de archivos spill + mapa en memoria
//	Fase 3: Escribe resultado final agregado
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
			// Incrementar contador
			counts[scanner.Text()]++

			// CHEQUEO DE MEMORIA: Si mapa crece mucho, hacer spill
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
			// Parsear linea "clave,contador" de archivo spill
			parts := strings.SplitN(scanner.Text(), ",", 2)
			if len(parts) == 2 {
				var val int
				fmt.Sscanf(parts[1], "%d", &val)
				// Acumular contador del spill
				counts[parts[0]] += val
			}
		}
		file.Close()
		os.Remove(spill) // Borrar archivo temporal
	}

	// Fase 3: Escritura Final
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	// Escribir resultado agregado "clave, contador"
	for k, v := range counts {
		w.WriteString(fmt.Sprintf("%s, %d\n", k, v))
	}
	return w.Flush()
}

// dumpSpill - Escribe mapa de contadores a archivo temporal
// Entrada: data - mapa clave->contador, filename - archivo destino
// Salida: error si falla escritura
// Descripcion: Serializa mapa a formato "clave,contador" en disco.
//
//	Usado para liberar memoria durante reduce de datasets grandes.
func dumpSpill(data map[string]int, filename string) error {
	// Crear archivo temporal de spill
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	// Escribir cada entrada del mapa
	for k, v := range data {
		w.WriteString(fmt.Sprintf("%s,%d\n", k, v))
	}
	return w.Flush()
}
