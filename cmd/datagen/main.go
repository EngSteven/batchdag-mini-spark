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

// Configuración
var (
	outDir     = "data"
	numSales   = flag.Int("sales", 1000000, "Cantidad de registros de ventas (aprox 50-100MB)")
	numCatalog = flag.Int("products", 1000, "Cantidad de productos en catálogo")
	linesText  = flag.Int("lines", 500000, "Líneas de texto para WordCount")
)

func main() {
	flag.Parse()
	os.MkdirAll(outDir, 0755)
	rand.Seed(time.Now().UnixNano())

	fmt.Println("=== Generador de Datasets Mini-Spark ===")
	
	// 1. Generar Dataset para JOIN (Ventas y Catálogo)
	generateJoinData()

	// 2. Generar Dataset para WordCount (Texto tipo Lorem Ipsum)
	generateTextData()

	fmt.Println("\n✅ Generación completada en carpeta 'data/'")
}

func generateJoinData() {
	fmt.Println("\n[1/3] Generando Catálogo (catalog.csv)...")
	fCat, _ := os.Create(fmt.Sprintf("%s/catalog.csv", outDir))
	defer fCat.Close()
	wCat := bufio.NewWriter(fCat)

	// Formato: product_id, product_name
	for i := 1; i <= *numCatalog; i++ {
		name := fmt.Sprintf("Producto_%d_Categoria_%c", i, rune('A'+rand.Intn(26)))
		wCat.WriteString(fmt.Sprintf("%d,%s\n", i, name))
	}
	wCat.Flush()

	fmt.Printf("[2/3] Generando Ventas (%d registros) (sales.csv)...\n", *numSales)
	fSales, _ := os.Create(fmt.Sprintf("%s/sales.csv", outDir))
	defer fSales.Close()
	wSales := bufio.NewWriter(fSales)

	// Formato: product_id, sale_details (fecha|monto)
	// NOTA: Nuestro Join simple usa la primera columna como clave.
	for i := 0; i < *numSales; i++ {
		prodID := rand.Intn(*numCatalog) + 1
		amount := rand.Float64() * 100.0
		date := time.Now().AddDate(0, 0, -rand.Intn(365)).Format("2006-01-02")
		// "product_id, fecha|monto"
		wSales.WriteString(fmt.Sprintf("%d,%s|$%.2f\n", prodID, date, amount))
	}
	wSales.Flush()
}

func generateTextData() {
	fmt.Printf("[3/3] Generando Texto Grande (%d líneas) (big_text.csv)...\n", *linesText)
	fText, _ := os.Create(fmt.Sprintf("%s/big_text.csv", outDir))
	defer fText.Close()
	wText := bufio.NewWriter(fText)

	words := []string{"lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit", "data", "spark", "go", "distributed", "system", "batch", "processing", "node", "network", "failure", "recovery"}

	for i := 0; i < *linesText; i++ {
		// Generar frase aleatoria
		numWords := rand.Intn(10) + 5
		var line []string
		for j := 0; j < numWords; j++ {
			line = append(line, words[rand.Intn(len(words))])
		}
		wText.WriteString(strings.Join(line, " ") + "\n")
	}
	wText.Flush()
}