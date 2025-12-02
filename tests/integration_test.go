/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: integration_test.go
Descripcion: Pruebas de integracion que validan pipelines completos.
             Simula flujo de datos a traves de multiples operadores
             encadenados (FlatMap -> Map -> ReduceByKey) sin usar
             infraestructura distribuida. Verifica composicion de ops.
*/

package tests

import (
	"mini-spark/internal/operators"
	"os"
	"strings"
	"testing"
)

// TestDataFlowIntegration - Prueba pipeline completo de word count
// Entrada: t - objeto testing
// Salida: ninguna (void), reporta fallos via t.Error
// Descripcion: Simula flujo de datos completo en un solo nodo:
//  1. FlatMap: tokenizar texto en palabras
//  2. Map: convertir a minusculas
//  3. ReduceByKey: contar frecuencias
//     Valida que pipeline produzca conteos correctos.
func TestDataFlowIntegration(t *testing.T) {
	// 1. Setup: Crear datos de prueba
	inputFile := createTempFile(t, "Hello World\nHello Go\nDistributed Systems")
	defer os.Remove(inputFile)

	// Archivos intermedios y final
	flatOut := inputFile + "_flat"
	mapOut := inputFile + "_map"
	reduceOut := inputFile + "_reduce"
	defer os.Remove(flatOut)
	defer os.Remove(mapOut)
	defer os.Remove(reduceOut)

	// 2. Ejecutar Pipeline (simulando workers secuenciales)

	// Stage 1: FlatMap - Tokenizar texto en palabras
	err := operators.FlatMap([]string{inputFile}, flatOut, "tokenize")
	if err != nil {
		t.Fatalf("FlatMap falló: %v", err)
	}

	// Stage 2: Map - Convertir palabras a minusculas
	err = operators.Map([]string{flatOut}, mapOut, "to_lower")
	if err != nil {
		t.Fatalf("Map falló: %v", err)
	}

	// Stage 3: ReduceByKey - Contar frecuencia de cada palabra
	err = operators.ReduceByKey([]string{mapOut}, reduceOut)
	if err != nil {
		t.Fatalf("Reduce falló: %v", err)
	}

	// 3. Validar Resultado Final
	finalRes := readFile(t, reduceOut)

	// Validar conteos esperados:
	// "Hello" aparece 2 veces en input -> "hello, 2"
	if !strings.Contains(finalRes, "hello, 2") {
		t.Errorf("Integration Test Falló. No se encontró 'hello, 2'. Output:\n%s", finalRes)
	}
	// "World" aparece 1 vez -> "world, 1"
	if !strings.Contains(finalRes, "world, 1") {
		t.Errorf("Falta 'world, 1'")
	}
}
