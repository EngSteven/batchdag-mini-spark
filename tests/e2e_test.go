package tests

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Helper para compilar
func buildBinaries(t *testing.T) {
	// IMPORTANTE: Aseguramos que 'make' se ejecute en la raíz del proyecto
	// Obtenemos el directorio actual donde corre el test
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("No se pudo obtener directorio actual: %v", err)
	}

	// Asumimos que estamos en /tests o en /raiz, buscamos el Makefile
	// Si estamos en /tests, la raíz es ".."
	projectRoot := filepath.Dir(wd)
	if _, err := os.Stat(filepath.Join(wd, "Makefile")); err == nil {
		projectRoot = wd // Estamos en la raíz
	}

	cmd := exec.Command("make", "build")
	cmd.Dir = projectRoot // <--- CLAVE: Ejecutar make en la raíz
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("Fallo al compilar binarios: %v", err)
	}
}

func TestEndToEndCluster(t *testing.T) {
	// 1. Compilar todo
	buildBinaries(t)

	// Ubicar los binarios correctamente (asumiendo que estamos en /tests o /raiz)
	// Como 'make build' crea 'bin/' en la raíz, necesitamos la ruta absoluta o relativa correcta
	wd, _ := os.Getwd()
	projectRoot := filepath.Dir(wd)
	if _, err := os.Stat(filepath.Join(wd, "Makefile")); err == nil {
		projectRoot = wd
	}
	
	masterBin := filepath.Join(projectRoot, "bin", "master")
	workerBin := filepath.Join(projectRoot, "bin", "worker")
	clientBin := filepath.Join(projectRoot, "bin", "client")

	// 2. Iniciar Master
	masterCmd := exec.Command(masterBin)
	// IMPORTANTE: Ejecutar procesos en la raíz para que encuentren 'data/' y escriban 'logs/'
	masterCmd.Dir = projectRoot 
	if err := masterCmd.Start(); err != nil { t.Fatal(err) }
	defer func() {
		if masterCmd.Process != nil { masterCmd.Process.Kill() }
	}()

	time.Sleep(2 * time.Second) // Esperar arranque

	// 3. Iniciar Worker
	workerCmd := exec.Command(workerBin, "-port", "9003")
	workerCmd.Dir = projectRoot
	if err := workerCmd.Start(); err != nil { t.Fatal(err) }
	defer func() {
		if workerCmd.Process != nil { workerCmd.Process.Kill() }
	}()

	time.Sleep(2 * time.Second) // Esperar registro

	// 4. Crear Job de Prueba temporal
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
	
	jobFile := filepath.Join(projectRoot, "e2e_job.json")
	os.WriteFile(jobFile, []byte(jobJSON), 0644)
	defer os.Remove(jobFile)

	// Asegurar datos dummy si no existen
	dataPath := filepath.Join(projectRoot, "data")
	os.MkdirAll(dataPath, 0755)
	os.WriteFile(filepath.Join(dataPath, "users.csv"), []byte("User1\nUser2"), 0644)

	// 5. Enviar Job con Cliente
	clientCmd := exec.Command(clientBin, "submit", jobFile)
	clientCmd.Dir = projectRoot
	out, err := clientCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Cliente falló: %v\nOutput: %s", err, string(out))
	}

	// 6. Esperar resultados (Polling básico)
	time.Sleep(5 * time.Second)

	// 7. Verificar archivo de salida
	// Buscamos en /tmp/mini-spark cualquier archivo que termine en _map.txt
	matches, _ := filepath.Glob("/tmp/mini-spark/*_map.txt")
	found := false
	for _, m := range matches {
		content, _ := os.ReadFile(m)
		sContent := string(content)
		if sContent != "" {
			// user1 (lowercase) debe existir
			if string(content) == "user1\nuser2\n" || string(content) == "user1\nuser2" {
				found = true
				break
			}
		}
	}

	if !found {
		t.Errorf("E2E Test Falló. No se encontró archivo de salida válido en /tmp/mini-spark")
	}
}