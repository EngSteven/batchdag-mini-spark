package tests

import (
	"mini-spark/internal/operators"
	"os"
	"strings"
	"testing"
)

// Simula un flujo de datos completo en un solo nodo
func TestDataFlowIntegration(t *testing.T) {
	// 1. Setup Datos
	inputFile := createTempFile(t, "Hello World\nHello Go\nDistributed Systems")
	defer os.Remove(inputFile)
	
	flatOut := inputFile + "_flat"
	mapOut := inputFile + "_map"
	reduceOut := inputFile + "_reduce"
	defer os.Remove(flatOut)
	defer os.Remove(mapOut)
	defer os.Remove(reduceOut)

	// 2. Ejecutar Pipeline (simulando lo que harían workers secuenciales)
	
	// Stage 1: FlatMap (Tokenize)
	err := operators.FlatMap([]string{inputFile}, flatOut, "tokenize")
	if err != nil { t.Fatalf("FlatMap falló: %v", err) }

	// Stage 2: Map (To Lower)
	err = operators.Map([]string{flatOut}, mapOut, "to_lower")
	if err != nil { t.Fatalf("Map falló: %v", err) }

	// Stage 3: Reduce (Count)
	err = operators.ReduceByKey([]string{mapOut}, reduceOut)
	if err != nil { t.Fatalf("Reduce falló: %v", err) }

	// 3. Validar Resultado Final
	finalRes := readFile(t, reduceOut)
	
	// "Hello" aparece 2 veces -> "hello, 2"
	if !strings.Contains(finalRes, "hello, 2") {
		t.Errorf("Integration Test Falló. No se encontró 'hello, 2'. Output:\n%s", finalRes)
	}
	if !strings.Contains(finalRes, "world, 1") {
		t.Errorf("Falta 'world, 1'")
	}
}