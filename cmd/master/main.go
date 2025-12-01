package main

import (
	"log"
	"mini-spark/internal/master"
	"mini-spark/internal/utils"
	"net/http"
)

func main() {
	m := master.NewMaster("master_state.json")
	m.LoadState()

	http.HandleFunc("/register", m.RegisterHandler)
	http.HandleFunc("/heartbeat", m.HeartbeatHandler)
	http.HandleFunc("/api/v1/jobs", m.SubmitJobHandler)
	http.HandleFunc("/api/v1/jobs/", m.GetJobStatusHandler)
	http.HandleFunc("/task/complete", m.CompleteTaskHandler)

	go m.HealthCheckLoop()
	go m.SchedulerLoop()

	utils.LogJSON("INFO", "Master iniciado", map[string]interface{}{"port": 8080})
	log.Fatal(http.ListenAndServe(":8080", nil))
}