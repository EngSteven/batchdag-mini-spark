package tests

import (
	"mini-spark/internal/operators"
	"os"
	"strings"
	"testing"
)

// --- HELPERS ---

func createTempFile(t *testing.T, content string) string {
	f, err := os.CreateTemp("", "test_data_*.txt")
	if err != nil { t.Fatal(err) }
	f.WriteString(content)
	f.Close()
	return f.Name()
}

func readFile(t *testing.T, path string) string {
	b, err := os.ReadFile(path)
	if err != nil { t.Fatal(err) }
	return strings.TrimSpace(string(b))
}

// --- TEST MAP (Table-Driven) ---

func TestOperatorMap(t *testing.T) {
	// Definimos la "Tabla" de casos de prueba
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

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inputFile := createTempFile(t, tc.input)
			defer os.Remove(inputFile)
			outputFile := inputFile + "_out"
			defer os.Remove(outputFile)

			err := operators.Map([]string{inputFile}, outputFile, tc.function)
			if err != nil {
				t.Fatalf("Error ejecutando Map: %v", err)
			}

			result := readFile(t, outputFile)
			if result != tc.expected {
				t.Errorf("\nCaso: %s\nEsperado:\n%s\nObtenido:\n%s", tc.name, tc.expected, result)
			}
		})
	}
}

// --- TEST REDUCE (Table-Driven) ---

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
			input:          "a\n\nb\n \n a", // Ojo: tu tokenizer actual podría necesitar ajustes si quieres ignorar esto
			expectedSubstr: []string{"a, 1"}, // Depende de cómo limpies los datos antes del reduce
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inputFile := createTempFile(t, tc.input)
			defer os.Remove(inputFile)
			outputFile := inputFile + "_out"
			defer os.Remove(outputFile)

			err := operators.ReduceByKey([]string{inputFile}, outputFile)
			if err != nil {
				t.Fatalf("Error ejecutando Reduce: %v", err)
			}

			result := readFile(t, outputFile)
			
			for _, exp := range tc.expectedSubstr {
				if !strings.Contains(result, exp) {
					t.Errorf("\nCaso: %s\nFalta el resultado esperado: '%s'\nOutput completo:\n%s", tc.name, exp, result)
				}
			}
		})
	}
}

// --- TEST FILTER (Nuevo, ya que agregamos el operador) ---

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

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inputFile := createTempFile(t, tc.input)
			defer os.Remove(inputFile)
			outputFile := inputFile + "_out"
			defer os.Remove(outputFile)

			err := operators.Filter([]string{inputFile}, outputFile, tc.function)
			if err != nil { t.Fatalf("Error Filter: %v", err) }

			result := readFile(t, outputFile)
			if result != tc.expected {
				t.Errorf("\nCaso: %s\nEsperado:\n'%s'\nObtenido:\n'%s'", tc.name, tc.expected, result)
			}
		})
	}
}

// --- TEST JOIN ---
func TestOperatorJoin(t *testing.T) {
	// Left: ID, Name
	leftContent := "1,Carlos\n2,Maria\n3,Juan"
	// Right: ID, Dept
	rightContent := "1,IT\n2,HR\n4,Sales"

	leftFile := createTempFile(t, leftContent)
	defer os.Remove(leftFile)
	rightFile := createTempFile(t, rightContent)
	defer os.Remove(rightFile)
	outputFile := leftFile + "_join_out"
	defer os.Remove(outputFile)

	err := operators.Join(leftFile, rightFile, outputFile)
	if err != nil { t.Fatalf("Join falló: %v", err) }

	result := readFile(t, outputFile)
	
	// Validaciones (Inner Join: solo 1 y 2 deben aparecer)
	if !strings.Contains(result, "1, Carlos, IT") { t.Error("Falta match 1") }
	if !strings.Contains(result, "2, Maria, HR") { t.Error("Falta match 2") }
	if strings.Contains(result, "Juan") { t.Error("Juan (3) no debería estar (no match en right)") }
	if strings.Contains(result, "Sales") { t.Error("Sales (4) no debería estar (no match en left)") }
}

// --- TEST READ/WRITE (Validar consistencia) ---
func TestOperatorReadCSV(t *testing.T) {
	content := "line1\nline2\nline3"
	in := createTempFile(t, content)
	defer os.Remove(in)
	out := in + "_copy"
	defer os.Remove(out)

	// ReadCSV actúa como una copia directa en nuestro modelo
	if err := operators.ReadCSV(in, out); err != nil { t.Fatal(err) }
	
	res := readFile(t, out)
	if res != content { t.Errorf("ReadCSV corrompió datos.\nEsp: %s\nObt: %s", content, res) }
}