/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: main.go (client)
Descripcion: Cliente CLI para Mini-Spark. Permite enviar trabajos,
             consultar estado y obtener resultados de jobs distribuidos.
             Proporciona interfaz de linea de comandos para interactuar
             con el nodo Master via API REST.
*/

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

// URL base del API REST del nodo Master
const baseURL = "http://localhost:8080/api/v1/jobs"

// main - Punto de entrada del cliente CLI
// Entrada: argumentos de linea de comandos (submit|status|results)
// Salida: ninguna (void), termina con exit code
// Descripcion: Parsea comandos CLI y delega a funciones especificas:
//   - submit: envia job definition al Master
//   - status: consulta progreso y metricas de job
//   - results: descarga archivos de salida finales
func main() {
	// Validar que se proporciono al menos un comando
	if len(os.Args) < 2 {
		printHelp()
		os.Exit(1)
	}

	// Extraer el comando solicitado
	command := os.Args[1]

	// Ejecutar accion segun comando
	switch command {
	case "submit":
		if len(os.Args) < 3 {
			log.Fatal("Uso: submit <archivo_job.json>")
		}
		submitJob(os.Args[2])
	case "status":
		if len(os.Args) < 3 {
			log.Fatal("Uso: status <job_id>")
		}
		getJobStatus(os.Args[2])
	case "results":
		if len(os.Args) < 3 {
			log.Fatal("Uso: results <job_id>")
		}
		getJobResults(os.Args[2])
	default:
		printHelp()
	}
}

// printHelp - Muestra mensaje de ayuda con comandos disponibles
// Entrada: ninguna
// Salida: ninguna (void)
// Descripcion: Imprime en stdout la sintaxis de uso del CLI
//
//	con ejemplos de cada comando disponible.
func printHelp() {
	fmt.Println("Uso de Mini-Spark CLI:")
	fmt.Println("  go run cmd/client/main.go submit <archivo.json>   -> Enviar nuevo trabajo")
	fmt.Println("  go run cmd/client/main.go status <job_id>         -> Ver estado y métricas")
	fmt.Println("  go run cmd/client/main.go results <job_id>        -> Ver archivos de salida")
}

// submitJob - Envia definicion de job al Master para ejecucion
// Entrada: filePath - ruta al archivo JSON con la definicion del job
// Salida: ninguna (void), imprime respuesta del Master
// Descripcion: Lee archivo JSON, lo envia via POST al endpoint /api/v1/jobs,
//
//	y muestra la respuesta formateada con el ID del job asignado.
func submitJob(filePath string) {
	// Leer contenido del archivo JSON
	jsonData, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error leyendo archivo: %v", err)
	}

	fmt.Printf("[CLI] Enviando job desde %s...\n", filePath)
	// Enviar peticion HTTP POST al Master
	resp, err := http.Post(baseURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatalf("Error conectando con Master: %v", err)
	}
	defer resp.Body.Close()

	// Leer respuesta del servidor
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Error del Master (%d): %s", resp.StatusCode, string(body))
	}

	// Formatear JSON de respuesta para mejor legibilidad
	var prettyJSON bytes.Buffer
	json.Indent(&prettyJSON, body, "", "  ")
	fmt.Printf("[CLI] Respuesta:\n%s\n", prettyJSON.String())
}

// getJobStatus - Consulta estado actual y metricas de un job
// Entrada: jobID - identificador unico del job
// Salida: ninguna (void), imprime estado en formato JSON
// Descripcion: Hace peticion GET al endpoint /api/v1/jobs/{id} y muestra:
//
//	estado, progreso porcentual, nodos completados, fallos.
func getJobStatus(jobID string) {
	// Construir URL con ID del job
	url := fmt.Sprintf("%s/%s", baseURL, jobID)
	// Realizar consulta HTTP GET
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error consultando estado: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Job no encontrado o error del servidor")
	}

	body, _ := io.ReadAll(resp.Body)
	var prettyJSON bytes.Buffer
	json.Indent(&prettyJSON, body, "", "  ")
	fmt.Printf("[CLI] Estado del Job %s:\n%s\n", jobID, prettyJSON.String())
}

// getJobResults - Descarga rutas de archivos de salida de un job
// Entrada: jobID - identificador unico del job
// Salida: ninguna (void), imprime rutas de archivos resultantes
// Descripcion: Consulta endpoint /api/v1/jobs/{id}/results para obtener
//
//	mapa de nodos finales (sink) y sus archivos de salida.
func getJobResults(jobID string) {
	// Construir URL para resultados
	url := fmt.Sprintf("%s/%s/results", baseURL, jobID)
	// Consultar resultados via GET
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error consultando resultados: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var prettyJSON bytes.Buffer
	json.Indent(&prettyJSON, body, "", "  ")
	fmt.Printf("[CLI] Resultados finales del Job %s:\n%s\n", jobID, prettyJSON.String())
}
