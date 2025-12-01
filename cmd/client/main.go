package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

const masterURL = "http://localhost:8080/api/v1/jobs"

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Uso: go run cmd/client/main.go <archivo_job.json>")
	}

	filePath := os.Args[1]
	jsonData, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Error leyendo archivo: %v", err)
	}

	fmt.Printf("[CLIENTE] Enviando job desde %s...\n", filePath)

	resp, err := http.Post(masterURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatalf("Error conectando con Master: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("El Master rechazó el job: %s", string(body))
	}

	fmt.Printf("[CLIENTE] Éxito! Respuesta del Master: %s\n", string(body))
}