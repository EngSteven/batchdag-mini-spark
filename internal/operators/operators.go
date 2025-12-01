package operators

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// --- UDFs (Funciones de Usuario) ---
var MapFunctions = map[string]func(string) string{
	"to_lower": func(s string) string { return strings.ToLower(s) },
	"to_json": func(s string) string {
		parts := strings.SplitN(s, ",", 2)
		if len(parts) < 2 { return "{}" }
		return fmt.Sprintf(`{"key": "%s", "value": "%s"}`, strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
	},
}

var FilterFunctions = map[string]func(string) bool{
	"long_words": func(s string) bool { return len(s) > 4 },
}

var FlatMapFunctions = map[string]func(string) []string{
	"tokenize": func(s string) []string {
		s = strings.Map(func(r rune) rune {
			if strings.ContainsRune(".,;?!-", r) { return -1 }
			return r
		}, s)
		return strings.Fields(s)
	},
}

// --- Operadores Core ---

func ReadCSV(inputPath, outputPath string) error {
	inFile, err := os.Open(inputPath)
	if err != nil { return err }
	defer inFile.Close()
	outFile, err := os.Create(outputPath)
	if err != nil { return err }
	defer outFile.Close()
	scanner := bufio.NewScanner(inFile)
	writer := bufio.NewWriter(outFile)
	for scanner.Scan() {
		writer.WriteString(scanner.Text() + "\n")
	}
	return writer.Flush()
}

func Map(inputs []string, output string, fnName string) error {
	fn, ok := MapFunctions[fnName]
	if !ok { return fmt.Errorf("fn map no encontrada: %s", fnName) }
	
	f, err := os.Create(output)
	if err != nil { return err }
	defer f.Close()
	w := bufio.NewWriter(f)

	for _, in := range inputs {
		file, _ := os.Open(in)
		if file == nil { continue }
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			w.WriteString(fn(scanner.Text()) + "\n")
		}
		file.Close()
	}
	return w.Flush()
}

func FlatMap(inputs []string, output string, fnName string) error {
	fn, ok := FlatMapFunctions[fnName]
	if !ok { return fmt.Errorf("fn flat_map no encontrada") }

	f, err := os.Create(output)
	if err != nil { return err }
	defer f.Close()
	w := bufio.NewWriter(f)

	for _, in := range inputs {
		file, _ := os.Open(in)
		if file == nil { continue }
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			for _, item := range fn(scanner.Text()) {
				w.WriteString(item + "\n")
			}
		}
		file.Close()
	}
	return w.Flush()
}

func Filter(inputs []string, output string, fnName string) error {
	fn, ok := FilterFunctions[fnName]
	if !ok { return fmt.Errorf("fn filter no encontrada") }

	f, err := os.Create(output)
	if err != nil { return err }
	defer f.Close()
	w := bufio.NewWriter(f)

	for _, in := range inputs {
		file, _ := os.Open(in)
		if file == nil { continue }
		scanner := bufio.NewScanner(file)
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

func ReduceByKey(inputs []string, output string) error {
	counts := make(map[string]int)
	for _, in := range inputs {
		file, err := os.Open(in)
		if err != nil { continue }
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			counts[scanner.Text()]++
		}
		file.Close()
	}
	f, err := os.Create(output)
	if err != nil { return err }
	defer f.Close()
	w := bufio.NewWriter(f)
	for k, v := range counts {
		w.WriteString(fmt.Sprintf("%s, %d\n", k, v))
	}
	return w.Flush()
}

func Join(leftFile, rightFile, output string) error {
	leftMap := make(map[string]string)
	lFile, err := os.Open(leftFile)
	if err != nil { return err }
	lScanner := bufio.NewScanner(lFile)
	for lScanner.Scan() {
		parts := strings.SplitN(lScanner.Text(), ",", 2)
		if len(parts) == 2 {
			leftMap[parts[0]] = parts[1]
		}
	}
	lFile.Close()

	rFile, err := os.Open(rightFile)
	if err != nil { return err }
	defer rFile.Close()
	
	outFile, err := os.Create(output)
	if err != nil { return err }
	defer outFile.Close()
	w := bufio.NewWriter(outFile)

	rScanner := bufio.NewScanner(rFile)
	for rScanner.Scan() {
		parts := strings.SplitN(rScanner.Text(), ",", 2)
		if len(parts) == 2 {
			key := parts[0]
			valRight := parts[1]
			if valLeft, ok := leftMap[key]; ok {
				w.WriteString(fmt.Sprintf("%s, %s, %s\n", key, valLeft, valRight))
			}
		}
	}
	return w.Flush()
}