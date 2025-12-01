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

const baseURL = "http://localhost:8080/api/v1/jobs"

func main() {
	if len(os.Args) < 2 {
		printHelp()
		os.Exit(1)
	}

	command := os.Args[1]

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

func printHelp() {
	fmt.Println("Uso de Mini-Spark CLI:")
	fmt.Println("  go run cmd/client/main.go submit <archivo.json>   -> Enviar nuevo trabajo")
	fmt.Println("  go run cmd/client/main.go status <job_id>         -> Ver estado y métricas")
	fmt.Println("  go run cmd/client/main.go results <job_id>        -> Ver archivos de salida")
}

func submitJob(filePath string) {
	jsonData, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error leyendo archivo: %v", err)
	}

	fmt.Printf("[CLI] Enviando job desde %s...\n", filePath)
	resp, err := http.Post(baseURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatalf("Error conectando con Master: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Error del Master (%d): %s", resp.StatusCode, string(body))
	}

	// Intentar formatear el JSON de respuesta bonito
	var prettyJSON bytes.Buffer
	json.Indent(&prettyJSON, body, "", "  ")
	fmt.Printf("[CLI] Respuesta:\n%s\n", prettyJSON.String())
}

func getJobStatus(jobID string) {
	url := fmt.Sprintf("%s/%s", baseURL, jobID)
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

func getJobResults(jobID string) {
	url := fmt.Sprintf("%s/%s/results", baseURL, jobID)
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