/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: main.go (worker)
Descripcion: Nodo trabajador de Mini-Spark. Ejecuta tareas asignadas
             por el Master, reporta metricas via heartbeats, y procesa
             operaciones distribuidas (map, reduce, join).
             Soporta auto-registro y recuperacion ante fallos.
*/

package main

import (
	"flag"
	"mini-spark/internal/utils"
	"mini-spark/internal/worker"
	"os"
)

// main - Punto de entrada del nodo Worker
// Entrada: flags --port (puerto HTTP del worker)
// Salida: ninguna (void), servidor HTTP bloqueante
// Descripcion: Inicializa worker, se registra en Master,
//
//	arranca servidor HTTP para recibir tareas,
//	y envia heartbeats periodicos con metricas.
func main() {
	// Parsear puerto del worker desde CLI
	port := flag.Int("port", 9001, "Puerto del worker")
	flag.Parse()

	// Obtener URL del Master desde variable de entorno
	masterURL := utils.GetEnv("MASTER_URL", "http://localhost:8080")
	// Directorio para archivos temporales de salida
	outputDir := "/tmp/mini-spark"
	// Crear directorio si no existe
	os.MkdirAll(outputDir, 0755)

	w := worker.NewWorker(*port, masterURL, outputDir)
	w.Start()
}
