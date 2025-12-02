/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: main.go (master)
Descripcion: Nodo coordinador principal del sistema Mini-Spark.
             Gestiona registro de workers, scheduling de tareas,
             monitoreo de salud de cluster, y API REST para clientes.
             Implementa tolerancia a fallos con reintentos automaticos.
*/

package main

import (
	"log"
	"mini-spark/internal/master"
	"mini-spark/internal/utils"
	"net/http"
)

// main - Punto de entrada del nodo Master
// Entrada: ninguna
// Salida: ninguna (void), servidor HTTP bloqueante
// Descripcion: Inicializa Master, registra endpoints HTTP, lanza
//
//	goroutines de background (health checks, scheduler)
//	y arranca servidor en puerto 8080.
func main() {
	// Crear instancia de Master con archivo de persistencia
	m := master.NewMaster("master_state.json")
	// Recuperar estado previo (jobs completados, outputs)
	m.LoadState()

	// Registrar endpoints de API REST
	http.HandleFunc("/register", m.RegisterHandler)          // Registro de workers
	http.HandleFunc("/heartbeat", m.HeartbeatHandler)        // Heartbeats de workers
	http.HandleFunc("/api/v1/jobs", m.SubmitJobHandler)      // Envio de jobs
	http.HandleFunc("/api/v1/jobs/", m.GetJobStatusHandler)  // Status/resultados
	http.HandleFunc("/task/complete", m.CompleteTaskHandler) // Completado de tareas

	// Lanzar loops de fondo en goroutines separadas
	go m.HealthCheckLoop() // Monitoreo de workers caidos
	go m.SchedulerLoop()   // Asignacion de tareas a workers

	utils.LogJSON("INFO", "Master iniciado", map[string]interface{}{"port": 8080})
	log.Fatal(http.ListenAndServe(":8080", nil))
}
