/*
Autores: Steven Sequeira Araya, Jefferson Salas Cordero
Nombre del archivo: main.go (datagen)
Descripcion: Generador de datasets sinteticos para pruebas de Mini-Spark.
             Crea archivos CSV grandes para benchmarking de operaciones
             distribuidas: JOIN (ventas + catalogo) y WordCount (texto).
             Parametrizable via flags de linea de comandos.
*/

package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

// Configuracion de generacion de datos
var (
	outDir     = "data" // Directorio de salida para archivos generados
	numSales   = flag.Int("sales", 1000000, "Cantidad de registros de ventas (aprox 50-100MB)")
	numCatalog = flag.Int("products", 1000, "Cantidad de productos en catálogo")
	linesText  = flag.Int("lines", 500000, "Líneas de texto para WordCount")
)

// main - Punto de entrada del generador de datasets
// Entrada: flags de CLI (--sales, --products, --lines)
// Salida: ninguna (void), crea archivos en carpeta 'data/'
// Descripcion: Genera 3 datasets de prueba:
//  1. catalog.csv - productos con IDs y nombres
//  2. sales.csv - transacciones de venta
//  3. big_text.csv - texto para WordCount
func main() {
	// Parsear argumentos de linea de comandos
	flag.Parse()
	// Crear directorio de salida si no existe
	os.MkdirAll(outDir, 0755)
	// Inicializar semilla aleatoria
	rand.Seed(time.Now().UnixNano())

	fmt.Println("=== Generador de Datasets Mini-Spark ===")

	// 1. Generar Dataset para JOIN (Ventas y Catálogo)
	generateJoinData()

	// 2. Generar Dataset para WordCount (Texto tipo Lorem Ipsum)
	generateTextData()

	fmt.Println("\n✅ Generación completada en carpeta 'data/'")
}

// generateJoinData - Genera datasets para pruebas de JOIN
// Entrada: ninguna (usa variables globales de configuracion)
// Salida: ninguna (void), crea catalog.csv y sales.csv
// Descripcion: Crea dos archivos relacionados por product_id:
//   - catalog.csv: product_id, product_name
//   - sales.csv: product_id, fecha|monto (millones de registros)
func generateJoinData() {
	fmt.Println("\n[1/3] Generando Catálogo (catalog.csv)...")
	// Crear archivo de catalogo
	fCat, _ := os.Create(fmt.Sprintf("%s/catalog.csv", outDir))
	defer fCat.Close()
	// Writer con buffer para escritura eficiente
	wCat := bufio.NewWriter(fCat)

	// Generar registros de catalogo: product_id, product_name
	for i := 1; i <= *numCatalog; i++ {
		// Nombre aleatorio con categoria (A-Z)
		name := fmt.Sprintf("Producto_%d_Categoria_%c", i, rune('A'+rand.Intn(26)))
		wCat.WriteString(fmt.Sprintf("%d,%s\n", i, name))
	}
	wCat.Flush()

	fmt.Printf("[2/3] Generando Ventas (%d registros) (sales.csv)...\n", *numSales)
	fSales, _ := os.Create(fmt.Sprintf("%s/sales.csv", outDir))
	defer fSales.Close()
	wSales := bufio.NewWriter(fSales)

	// Generar registros de ventas: product_id, fecha|monto
	// NOTA: Primera columna es clave para JOIN
	for i := 0; i < *numSales; i++ {
		// ID de producto aleatorio del catalogo
		prodID := rand.Intn(*numCatalog) + 1
		// Monto aleatorio entre $0 y $100
		amount := rand.Float64() * 100.0
		// Fecha aleatoria del ultimo año
		date := time.Now().AddDate(0, 0, -rand.Intn(365)).Format("2006-01-02")
		// Escribir linea: "product_id, fecha|monto"
		wSales.WriteString(fmt.Sprintf("%d,%s|$%.2f\n", prodID, date, amount))
	}
	wSales.Flush()
}

// generateTextData - Genera texto sintetico para WordCount
// Entrada: ninguna (usa variable global linesText)
// Salida: ninguna (void), crea big_text.csv
// Descripcion: Genera archivo de texto con lineas aleatorias usando
//
//	vocabulario fijo. Usado para pruebas de FlatMap + ReduceByKey.
func generateTextData() {
	fmt.Printf("[3/3] Generando Texto Grande (%d líneas) (big_text.csv)...\n", *linesText)
	// Crear archivo de texto
	fText, _ := os.Create(fmt.Sprintf("%s/big_text.csv", outDir))
	defer fText.Close()
	wText := bufio.NewWriter(fText)

	// Vocabulario fijo para generacion de texto
	words := []string{"lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit", "data", "spark", "go", "distributed", "system", "batch", "processing", "node", "network", "failure", "recovery"}

	// Generar lineas de texto aleatorias
	for i := 0; i < *linesText; i++ {
		// Numero aleatorio de palabras por linea (5-14)
		numWords := rand.Intn(10) + 5
		var line []string
		// Seleccionar palabras aleatorias del vocabulario
		for j := 0; j < numWords; j++ {
			line = append(line, words[rand.Intn(len(words))])
		}
		// Escribir linea con palabras separadas por espacios
		wText.WriteString(strings.Join(line, " ") + "\n")
	}
	wText.Flush()
}
