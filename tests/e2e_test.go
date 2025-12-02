/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: e2e_test.go
Descripcion: Pruebas end-to-end del sistema distribuido completo.
             Levanta cluster real (Master + Worker), envia jobs via CLI,
             y valida que archivos de salida se generen correctamente.
             Simula escenario de produccion con multiples procesos.
*/

package tests

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// buildBinaries - Compila binarios del proyecto usando Makefile
// Entrada: t - objeto testing
// Salida: ninguna (void), falla test si compilacion falla
// Descripcion: Localiza raiz del proyecto (busca Makefile),
//
//	ejecuta 'make build' para compilar master, worker y client.
//	Maneja casos donde test se ejecuta desde /tests o /raiz.
func buildBinaries(t *testing.T) {
	// Obtener directorio actual del test
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("No se pudo obtener directorio actual: %v", err)
	}

	// Localizar raiz del proyecto buscando Makefile
	// Si estamos en /tests, la raiz es el directorio padre
	projectRoot := filepath.Dir(wd)
	if _, err := os.Stat(filepath.Join(wd, "Makefile")); err == nil {
		projectRoot = wd // Ya estamos en la raiz
	}

	// Ejecutar make build en la raiz del proyecto
	cmd := exec.Command("make", "build")
	cmd.Dir = projectRoot // CLAVE: Ejecutar desde raiz del proyecto
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("Fallo al compilar binarios: %v", err)
	}
}

// TestEndToEndCluster - Prueba sistema distribuido completo
// Entrada: t - objeto testing
// Salida: ninguna (void), reporta fallos via t.Error
// Descripcion: Test end-to-end que:
//  1. Compila binarios (master, worker, client)
//  2. Levanta nodo Master
//  3. Levanta nodo Worker y lo registra
//  4. Envia job via CLI client
//  5. Valida archivos de salida generados
//     Simula escenario de produccion con cluster real.
func TestEndToEndCluster(t *testing.T) {
	// 1. Compilar todos los binarios
	buildBinaries(t)

	// Localizar binarios compilados en bin/
	wd, _ := os.Getwd()
	projectRoot := filepath.Dir(wd)
	if _, err := os.Stat(filepath.Join(wd, "Makefile")); err == nil {
		projectRoot = wd
	}

	// Paths absolutos a binarios
	masterBin := filepath.Join(projectRoot, "bin", "master")
	workerBin := filepath.Join(projectRoot, "bin", "worker")
	clientBin := filepath.Join(projectRoot, "bin", "client")

	// 2. Iniciar nodo Master en background
	masterCmd := exec.Command(masterBin)
	// Ejecutar desde raiz para acceder a data/ y crear logs/
	masterCmd.Dir = projectRoot
	if err := masterCmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Cleanup: matar proceso al final del test
	defer func() {
		if masterCmd.Process != nil {
			masterCmd.Process.Kill()
		}
	}()

	time.Sleep(2 * time.Second) // Esperar arranque del Master

	// 3. Iniciar nodo Worker en background
	workerCmd := exec.Command(workerBin, "-port", "9003")
	workerCmd.Dir = projectRoot
	if err := workerCmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Cleanup: matar proceso al final
	defer func() {
		if workerCmd.Process != nil {
			workerCmd.Process.Kill()
		}
	}()

	time.Sleep(2 * time.Second) // Esperar registro del Worker en Master

	// 4. Crear Job de Prueba temporal
	// DAG simple: read_csv -> map (to_lower)
	jobJSON := `
	{
		"name": "e2e-test",
		"dag": {
			"nodes": [
				{"id": "read", "op": "read_csv", "path": "data/users.csv"},
				{"id": "map", "op": "map", "fn": "to_lower"}
			],
			"edges": [["read", "map"]]
		},
		"parallelism": 1
	}`

	// Escribir definicion de job a archivo temporal
	jobFile := filepath.Join(projectRoot, "e2e_job.json")
	os.WriteFile(jobFile, []byte(jobJSON), 0644)
	defer os.Remove(jobFile)

	// Setup: Crear datos de entrada si no existen
	dataPath := filepath.Join(projectRoot, "data")
	os.MkdirAll(dataPath, 0755)
	os.WriteFile(filepath.Join(dataPath, "users.csv"), []byte("User1\nUser2"), 0644)

	// 5. Enviar Job usando cliente CLI
	clientCmd := exec.Command(clientBin, "submit", jobFile)
	clientCmd.Dir = projectRoot
	out, err := clientCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Cliente falló: %v\nOutput: %s", err, string(out))
	}

	// 6. Esperar procesamiento del job (polling basico)
	time.Sleep(5 * time.Second)

	// 7. Verificar archivo de salida generado por Worker
	// Buscar archivos _map.txt en /tmp/mini-spark (directorio de salida del worker)
	matches, _ := filepath.Glob("/tmp/mini-spark/*_map.txt")
	found := false
	for _, m := range matches {
		content, _ := os.ReadFile(m)
		sContent := string(content)
		if sContent != "" {
			// Validar que contenga datos transformados (lowercase)
			// "User1" -> "user1", "User2" -> "user2"
			if string(content) == "user1\nuser2\n" || string(content) == "user1\nuser2" {
				found = true
				break
			}
		}
	}

	// Fallar test si no se encontro archivo de salida valido
	if !found {
		t.Errorf("E2E Test Falló. No se encontró archivo de salida válido en /tmp/mini-spark")
	}
}
