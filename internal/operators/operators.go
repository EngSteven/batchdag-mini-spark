/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: operators.go
Descripcion: Implementacion de operadores distribuidos tipo Spark.
             Incluye transformaciones (map, flatMap, filter),
             acciones (reduceByKey), E/S (read_csv) y joins.
             Cada operador trabaja con archivos intermedios para
             permitir paralelizacion y tolerancia a fallos.
*/

package operators

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// --- UDFs (Funciones de Usuario) ---
// MapFunctions - Registro de funciones map disponibles
// Cada funcion toma una linea de texto y retorna linea transformada
var MapFunctions = map[string]func(string) string{
	"to_lower": func(s string) string { return strings.ToLower(s) },
	"to_json": func(s string) string {
		// Convierte CSV "key,value" a JSON {"key": "...", "value": "..."}
		parts := strings.SplitN(s, ",", 2)
		if len(parts) < 2 {
			return "{}"
		}
		return fmt.Sprintf(`{"key": "%s", "value": "%s"}`, strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
	},
}

// FilterFunctions - Registro de funciones filter (predicados)
// Cada funcion toma una linea y retorna true si pasa el filtro
var FilterFunctions = map[string]func(string) bool{
	"long_words": func(s string) bool { return len(s) > 4 },
}

// FlatMapFunctions - Registro de funciones flatMap
// Cada funcion toma una linea y retorna slice de strings
var FlatMapFunctions = map[string]func(string) []string{
	"tokenize": func(s string) []string {
		// Eliminar puntuacion y dividir en palabras
		s = strings.Map(func(r rune) rune {
			if strings.ContainsRune(".,;?!-", r) {
				return -1
			}
			return r
		}, s)
		return strings.Fields(s)
	},
}

// --- Operadores Core ---

// ReadCSV - Lee archivo de texto/CSV linea por linea
// Entrada: inputPath - archivo fuente, outputPath - archivo destino
// Salida: error si falla lectura/escritura
// Descripcion: Copia archivo de entrada a salida sin transformacion.
//
//	Usado como nodo source en DAGs.
func ReadCSV(inputPath, outputPath string) error {
	// Abrir archivo de entrada
	inFile, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer inFile.Close()
	// Crear archivo de salida
	outFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outFile.Close()
	// Copiar linea por linea
	scanner := bufio.NewScanner(inFile)
	writer := bufio.NewWriter(outFile)
	for scanner.Scan() {
		writer.WriteString(scanner.Text() + "\n")
	}
	return writer.Flush()
}

// Map - Aplica funcion UDF a cada linea de archivos de entrada
// Entrada: inputs - slice de archivos, output - archivo destino, fnName - nombre de UDF
// Salida: error si funcion no existe o falla I/O
// Descripcion: Lee todos los archivos de entrada, aplica funcion de transformacion
//
//	registrada en MapFunctions, escribe lineas transformadas a salida.
func Map(inputs []string, output string, fnName string) error {
	// Buscar funcion registrada
	fn, ok := MapFunctions[fnName]
	if !ok {
		return fmt.Errorf("fn map no encontrada: %s", fnName)
	}

	// Crear archivo de salida
	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	// Procesar cada archivo de entrada
	for _, in := range inputs {
		file, _ := os.Open(in)
		if file == nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		// Aplicar funcion a cada linea
		for scanner.Scan() {
			w.WriteString(fn(scanner.Text()) + "\n")
		}
		file.Close()
	}
	return w.Flush()
}

// FlatMap - Aplica funcion que retorna multiples valores por linea de entrada
// Entrada: inputs - slice de archivos, output - archivo destino, fnName - nombre de UDF
// Salida: error si funcion no existe o falla I/O
// Descripcion: Similar a Map pero cada linea puede generar 0 o mas lineas de salida.
//
//	Usado tipicamente para tokenizacion (ej: texto -> palabras).
func FlatMap(inputs []string, output string, fnName string) error {
	// Buscar funcion registrada
	fn, ok := FlatMapFunctions[fnName]
	if !ok {
		return fmt.Errorf("fn flat_map no encontrada")
	}

	// Crear archivo de salida
	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	// Procesar cada archivo de entrada
	for _, in := range inputs {
		file, _ := os.Open(in)
		if file == nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		// Aplicar funcion y escribir todos los items generados
		for scanner.Scan() {
			for _, item := range fn(scanner.Text()) {
				w.WriteString(item + "\n")
			}
		}
		file.Close()
	}
	return w.Flush()
}

// Filter - Filtra lineas segun predicado booleano
// Entrada: inputs - slice de archivos, output - archivo destino, fnName - nombre de UDF
// Salida: error si funcion no existe o falla I/O
// Descripcion: Aplica funcion predicado a cada linea. Solo lineas que retornan
//
//	true se escriben a salida. Reduce volumen de datos.
func Filter(inputs []string, output string, fnName string) error {
	// Buscar funcion predicado
	fn, ok := FilterFunctions[fnName]
	if !ok {
		return fmt.Errorf("fn filter no encontrada")
	}

	// Crear archivo de salida
	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	// Procesar cada archivo de entrada
	for _, in := range inputs {
		file, _ := os.Open(in)
		if file == nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		// Aplicar predicado y escribir solo lineas que pasan
		for scanner.Scan() {
			line := scanner.Text()
			if fn(line) {
				w.WriteString(line + "\n")
			}
		}
		file.Close()
	}
	return w.Flush()
}

// ReduceByKey - Agrega valores por clave (conteo de palabras)
// Entrada: inputs - slice de archivos con claves, output - archivo destino
// Salida: error si falla I/O
// Descripcion: Cuenta frecuencia de cada linea (usada como clave).
//
//	Lee todos los inputs, mantiene mapa en memoria,
//	escribe resultado "clave, contador". Operacion shuffle.
func ReduceByKey(inputs []string, output string) error {
	// Mapa en memoria para conteo
	counts := make(map[string]int)
	// Leer y contar todas las claves
	for _, in := range inputs {
		file, err := os.Open(in)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			// Incrementar contador de la clave
			counts[scanner.Text()]++
		}
		file.Close()
	}
	// Escribir resultados agregados
	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for k, v := range counts {
		w.WriteString(fmt.Sprintf("%s, %d\n", k, v))
	}
	return w.Flush()
}

// Join - Realiza inner join de dos archivos CSV por primera columna
// Entrada: leftFile - archivo izquierdo, rightFile - archivo derecho, output - destino
// Salida: error si falla I/O
// Descripcion: Carga leftFile completo en memoria como mapa (clave -> valor).
//
//	Itera rightFile y busca coincidencias, escribiendo join result.
//	Formato salida: "clave, valor_left, valor_right"
func Join(leftFile, rightFile, output string) error {
	// Cargar archivo izquierdo en mapa (hash join)
	leftMap := make(map[string]string)
	lFile, err := os.Open(leftFile)
	if err != nil {
		return err
	}
	lScanner := bufio.NewScanner(lFile)
	for lScanner.Scan() {
		// Parsear linea como "clave, valor"
		parts := strings.SplitN(lScanner.Text(), ",", 2)
		if len(parts) == 2 {
			leftMap[parts[0]] = parts[1]
		}
	}
	lFile.Close()

	// Abrir archivo derecho
	rFile, err := os.Open(rightFile)
	if err != nil {
		return err
	}
	defer rFile.Close()

	// Crear archivo de salida
	outFile, err := os.Create(output)
	if err != nil {
		return err
	}
	defer outFile.Close()
	w := bufio.NewWriter(outFile)

	// Iterar archivo derecho y buscar coincidencias
	rScanner := bufio.NewScanner(rFile)
	for rScanner.Scan() {
		parts := strings.SplitN(rScanner.Text(), ",", 2)
		if len(parts) == 2 {
			key := parts[0]
			valRight := parts[1]
			// Si hay match, escribir join result
			if valLeft, ok := leftMap[key]; ok {
				w.WriteString(fmt.Sprintf("%s, %s, %s\n", key, valLeft, valRight))
			}
		}
	}
	return w.Flush()
}
