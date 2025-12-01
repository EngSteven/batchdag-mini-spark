package main

import (
	"flag"
	"mini-spark/internal/utils"
	"mini-spark/internal/worker"
	"os"
)

func main() {
	port := flag.Int("port", 9001, "Puerto del worker")
	flag.Parse()

	masterURL := utils.GetEnv("MASTER_URL", "http://localhost:8080")
	outputDir := "/tmp/mini-spark"
	os.MkdirAll(outputDir, 0755)

	w := worker.NewWorker(*port, masterURL, outputDir)
	w.Start()
}