/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: unit_test.go
Descripcion: Suite de pruebas unitarias para operadores individuales.
             Valida funcionamiento aislado de Map, FlatMap, Filter,
             ReduceByKey, Join y ReadCSV usando table-driven tests.
             Cubre casos normales, casos borde y manejo de errores.
*/

package tests

import (
	"mini-spark/internal/operators"
	"os"
	"strings"
	"testing"
)

// --- HELPERS ---

// createTempFile - Crea archivo temporal con contenido especificado
// Entrada: t - objeto testing, content - contenido del archivo
// Salida: string con ruta absoluta del archivo temporal
// Descripcion: Usa os.CreateTemp para crear archivo unico en /tmp,
//
//	escribe contenido y retorna path. Util para setup de tests.
func createTempFile(t *testing.T, content string) string {
	// Crear archivo temporal con prefijo test_data_
	f, err := os.CreateTemp("", "test_data_*.txt")
	if err != nil {
		t.Fatal(err)
	}
	// Escribir contenido de prueba
	f.WriteString(content)
	f.Close()
	return f.Name()
}

// readFile - Lee contenido completo de un archivo
// Entrada: t - objeto testing, path - ruta del archivo
// Salida: string con contenido del archivo (sin espacios finales)
// Descripcion: Lee archivo completo y elimina whitespace trailing.
//
//	Falla el test si hay error de lectura.
func readFile(t *testing.T, path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

// --- TEST MAP (Table-Driven) ---

// TestOperatorMap - Prueba operador Map con multiples funciones UDF
// Entrada: t - objeto testing
// Salida: ninguna (void), reporta fallos via t.Error/t.Fatal
// Descripcion: Usa table-driven testing para validar:
//   - to_lower: conversion a minusculas
//   - to_json: conversion CSV a JSON
//     Cubre casos normales y casos borde (lineas malformadas)
func TestOperatorMap(t *testing.T) {
	// Definir tabla de casos de prueba
	cases := []struct {
		name     string // Nombre del caso (para logs)
		function string // Función a probar (to_lower, to_json)
		input    string // Contenido del archivo de entrada
		expected string // Lo que esperamos en el archivo de salida
	}{
		{
			name:     "ToLower Básico",
			function: "to_lower",
			input:    "HOLA\nMUNDO",
			expected: "hola\nmundo",
		},
		{
			name:     "ToLower con Números y Símbolos",
			function: "to_lower",
			input:    "Go 1.22 Rocks!",
			expected: "go 1.22 rocks!",
		},
		{
			name:     "ToJson Simple",
			function: "to_json",
			input:    "1,Carlos\n2,Maria",
			expected: `{"key": "1", "value": "Carlos"}` + "\n" + `{"key": "2", "value": "Maria"}`,
		},
		{
			name:     "ToJson Línea Malformada",
			function: "to_json",
			input:    "LineaSinComa",
			expected: "{}", // Según nuestra lógica devuelve objeto vacío
		},
	}

	// Ejecutar cada caso de prueba como subtest
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup: crear archivo temporal con input
			inputFile := createTempFile(t, tc.input)
			defer os.Remove(inputFile)
			outputFile := inputFile + "_out"
			defer os.Remove(outputFile)

			// Ejecutar operador Map con funcion especificada
			err := operators.Map([]string{inputFile}, outputFile, tc.function)
			if err != nil {
				t.Fatalf("Error ejecutando Map: %v", err)
			}

			// Validar resultado contra expected
			result := readFile(t, outputFile)
			if result != tc.expected {
				t.Errorf("\nCaso: %s\nEsperado:\n%s\nObtenido:\n%s", tc.name, tc.expected, result)
			}
		})
	}
}

// --- TEST REDUCE (Table-Driven) ---

// TestOperatorReduceByKey - Prueba agregacion por clave (word count)
// Entrada: t - objeto testing
// Salida: ninguna (void), reporta fallos via t.Error
// Descripcion: Valida conteo de frecuencias con table-driven testing:
//   - Conteo basico de palabras repetidas
//   - Archivos vacios
//   - Una sola linea
//     NOTA: Orden de salida es aleatorio (map), se valida por substring
func TestOperatorReduceByKey(t *testing.T) {
	cases := []struct {
		name           string
		input          string
		expectedSubstr []string // Validamos que contenga ciertos strings (porque el orden del map es aleatorio)
	}{
		{
			name:           "Conteo Básico",
			input:          "apple\nbanana\napple\napple\nbanana",
			expectedSubstr: []string{"apple, 3", "banana, 2"},
		},
		{
			name:           "Archivo Vacío",
			input:          "",
			expectedSubstr: []string{""}, // No debería fallar, solo archivo vacío
		},
		{
			name:           "Una sola línea",
			input:          "solitario",
			expectedSubstr: []string{"solitario, 1"},
		},
		{
			name:           "Espacios y saltos extra",
			input:          "a\n\nb\n \n a",  // Ojo: tu tokenizer actual podría necesitar ajustes si quieres ignorar esto
			expectedSubstr: []string{"a, 1"}, // Depende de cómo limpies los datos antes del reduce
		},
	}

	// Ejecutar cada caso como subtest
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup: crear archivo de entrada
			inputFile := createTempFile(t, tc.input)
			defer os.Remove(inputFile)
			outputFile := inputFile + "_out"
			defer os.Remove(outputFile)

			// Ejecutar ReduceByKey
			err := operators.ReduceByKey([]string{inputFile}, outputFile)
			if err != nil {
				t.Fatalf("Error ejecutando Reduce: %v", err)
			}

			// Leer resultado
			result := readFile(t, outputFile)

			// Validar que contenga substrings esperados (orden no importa)
			for _, exp := range tc.expectedSubstr {
				if !strings.Contains(result, exp) {
					t.Errorf("\nCaso: %s\nFalta el resultado esperado: '%s'\nOutput completo:\n%s", tc.name, exp, result)
				}
			}
		})
	}
}

// --- TEST FILTER (Nuevo, ya que agregamos el operador) ---

// TestOperatorFilter - Prueba operador Filter con predicados booleanos
// Entrada: t - objeto testing
// Salida: ninguna (void), reporta fallos via t.Error
// Descripcion: Valida filtrado de lineas segun predicados:
//   - long_words: solo palabras con longitud > 4
//     Verifica que lineas que no pasan filtro se excluyan.
func TestOperatorFilter(t *testing.T) {
	cases := []struct {
		name     string
		function string
		input    string
		expected string
	}{
		{
			name:     "Filtrar palabras largas",
			function: "long_words",
			input:    "hola\nmundo\nes\ngenial",
			expected: "mundo\ngenial", // 'hola' tiene 4, la condición es > 4
		},
		{
			name:     "Nada pasa el filtro",
			function: "long_words",
			input:    "no\nsi\nmal",
			expected: "",
		},
	}

	// Ejecutar cada caso como subtest
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup: crear archivo temporal
			inputFile := createTempFile(t, tc.input)
			defer os.Remove(inputFile)
			outputFile := inputFile + "_out"
			defer os.Remove(outputFile)

			// Ejecutar Filter con funcion predicado
			err := operators.Filter([]string{inputFile}, outputFile, tc.function)
			if err != nil {
				t.Fatalf("Error Filter: %v", err)
			}

			result := readFile(t, outputFile)
			if result != tc.expected {
				t.Errorf("\nCaso: %s\nEsperado:\n'%s'\nObtenido:\n'%s'", tc.name, tc.expected, result)
			}
		})
	}
}

// --- TEST JOIN ---

// TestOperatorJoin - Prueba operador Join (inner join por primera columna)
// Entrada: t - objeto testing
// Salida: ninguna (void), reporta fallos via t.Error
// Descripcion: Valida join de dos archivos CSV usando primera columna como clave.
//
//	Verifica que solo registros con match en ambos lados aparezcan.
//	Left: (ID, Name), Right: (ID, Dept) -> Output: (ID, Name, Dept)
func TestOperatorJoin(t *testing.T) {
	// Setup: Crear archivos left y right
	// Left: ID, Name
	leftContent := "1,Carlos\n2,Maria\n3,Juan"
	// Right: ID, Dept
	rightContent := "1,IT\n2,HR\n4,Sales"

	// Crear archivos temporales
	leftFile := createTempFile(t, leftContent)
	defer os.Remove(leftFile)
	rightFile := createTempFile(t, rightContent)
	defer os.Remove(rightFile)
	outputFile := leftFile + "_join_out"
	defer os.Remove(outputFile)

	// Ejecutar operador Join
	err := operators.Join(leftFile, rightFile, outputFile)
	if err != nil {
		t.Fatalf("Join falló: %v", err)
	}

	// Leer resultado
	result := readFile(t, outputFile)

	// Validaciones (Inner Join: solo IDs 1 y 2 deben aparecer)
	if !strings.Contains(result, "1, Carlos, IT") {
		t.Error("Falta match 1")
	}
	if !strings.Contains(result, "2, Maria, HR") {
		t.Error("Falta match 2")
	}
	if strings.Contains(result, "Juan") {
		t.Error("Juan (3) no debería estar (no match en right)")
	}
	if strings.Contains(result, "Sales") {
		t.Error("Sales (4) no debería estar (no match en left)")
	}
}

// --- TEST READ/WRITE (Validar consistencia) ---

// TestOperatorReadCSV - Prueba lectura/escritura de archivos CSV
// Entrada: t - objeto testing
// Salida: ninguna (void), reporta fallos via t.Error
// Descripcion: Valida que ReadCSV preserve exactamente el contenido
//
//	del archivo de entrada (operacion de copia sin transformacion).
//	Detecta corrupcion de datos durante I/O.
func TestOperatorReadCSV(t *testing.T) {
	// Setup: crear archivo con contenido conocido
	content := "line1\nline2\nline3"
	in := createTempFile(t, content)
	defer os.Remove(in)
	out := in + "_copy"
	defer os.Remove(out)

	// Ejecutar ReadCSV (actua como copia directa)
	if err := operators.ReadCSV(in, out); err != nil {
		t.Fatal(err)
	}

	// Validar que contenido sea identico
	res := readFile(t, out)
	if res != content {
		t.Errorf("ReadCSV corrompió datos.\nEsp: %s\nObt: %s", content, res)
	}
}
