package master

import (
	"encoding/json"
	"fmt"
	"mini-spark/internal/common"
	"mini-spark/internal/utils"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

func (m *Master) RegisterHandler(w http.ResponseWriter, r *http.Request) {
	var req common.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil { host = "localhost" }
	if host == "::1" || host == "127.0.0.1" { host = "localhost" }

	workerURL := fmt.Sprintf("http://%s:%d", host, req.Port)
	m.Workers[req.ID] = &common.WorkerInfo{
		ID:            req.ID,
		URL:           workerURL,
		LastHeartbeat: time.Now(),
		Status:        "UP",
	}

	utils.LogJSON("INFO", "Worker registrado", map[string]interface{}{"worker_id": req.ID, "url": workerURL})
	w.WriteHeader(http.StatusOK)
}

func (m *Master) HeartbeatHandler(w http.ResponseWriter, r *http.Request) {
	var req common.HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest); return
	}
	m.mu.Lock()
	if worker, exists := m.Workers[req.ID]; exists {
		worker.LastHeartbeat = time.Now(); worker.Status = "UP"; worker.Metrics = req.Metrics
	}
	m.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (m *Master) SubmitJobHandler(w http.ResponseWriter, r *http.Request) {
	var req common.JobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "JSON inválido", http.StatusBadRequest); return
	}

	jobID := uuid.New().String()
	job := &common.Job{ID: jobID, Name: req.Name, Status: "RUNNING", Graph: req.DAG, Submitted: time.Now()}

	m.mu.Lock()
	m.Jobs[jobID] = job
	m.InitJobProgress(job)
	m.SaveState()
	m.mu.Unlock()

	utils.LogJSON("INFO", "Job recibido", map[string]interface{}{"job_id": jobID})
	go m.ScheduleSourceTasks(job)
	json.NewEncoder(w).Encode(map[string]string{"job_id": jobID, "status": "ACCEPTED"})
}

func (m *Master) GetJobStatusHandler(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 { http.Error(w, "ID no encontrado", http.StatusBadRequest); return }
	jobID := parts[4]
	if len(parts) >= 6 && parts[5] == "results" { m.GetJobResultsHandler(w, r, jobID); return }

	m.mu.Lock()
	job, exists := m.Jobs[jobID]
	progressMap := make(map[string]string)
	completedCount := 0
	totalNodes := 0
	if p, ok := m.JobProgress[jobID]; ok {
		totalNodes = len(p)
		for k, v := range p {
			progressMap[k] = v
			if v == "COMPLETED" { completedCount++ }
		}
	}
	failures := m.JobFailures[jobID]
	m.mu.Unlock()

	if !exists { http.Error(w, "Job no encontrado", http.StatusNotFound); return }
	duration := time.Since(job.Submitted).Seconds()
	if job.Status == "COMPLETED" || job.Status == "FAILED" { duration = job.Completed.Sub(job.Submitted).Seconds() }
	
	progressPercent := 0.0
	if totalNodes > 0 { progressPercent = (float64(completedCount) / float64(totalNodes)) * 100 }

	json.NewEncoder(w).Encode(common.JobStatusResponse{
		ID: job.ID, Name: job.Name, Status: job.Status, Submitted: job.Submitted,
		DurationSecs: duration, Progress: progressPercent, NodeStatus: progressMap, Failures: failures,
	})
}

func (m *Master) GetJobResultsHandler(w http.ResponseWriter, r *http.Request, jobID string) {
	m.mu.Lock()
	_, exists := m.Jobs[jobID]
	outputs := make(map[string]string)
	if outs, ok := m.JobOutputs[jobID]; ok { for k, v := range outs { outputs[k] = v } }
	m.mu.Unlock()

	if !exists { http.Error(w, "Job no encontrado", http.StatusNotFound); return }

	outDegree := make(map[string]int)
	for _, node := range m.Jobs[jobID].Graph.Nodes { outDegree[node.ID] = 0 }
	for _, edge := range m.Jobs[jobID].Graph.Edges { outDegree[edge[0]]++ }

	finalOutputs := make(map[string]string)
	for nodeID, count := range outDegree {
		if count == 0 { if path, ok := outputs[nodeID]; ok { finalOutputs[nodeID] = path } }
	}
	json.NewEncoder(w).Encode(common.JobResultsResponse{JobID: jobID, Outputs: finalOutputs})
}

func (m *Master) CompleteTaskHandler(w http.ResponseWriter, r *http.Request) {
	var res common.TaskResult
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil { http.Error(w, "Bad Request", http.StatusBadRequest); return }

	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.TaskAssignments, res.ID)
	originalTask, taskFound := m.RunningTasks[res.ID]
	delete(m.RunningTasks, res.ID)

	if res.Status == "FAILED" {
		m.JobFailures[res.JobID]++
		utils.LogJSON("ERROR", "Fallo en tarea", map[string]interface{}{"node": res.NodeID, "error": res.ErrorMsg})
		if taskFound && originalTask.Attempt < common.MaxRetries {
			originalTask.Attempt++
			originalTask.ID = uuid.New().String()
			go func(t common.Task) { m.TaskQueue <- t }(originalTask)
		} else {
			if job, ok := m.Jobs[res.JobID]; ok { job.Status = "FAILED"; job.Completed = time.Now() }
		}
		m.SaveState()
		w.WriteHeader(http.StatusOK)
		return
	}

	m.setNodeStatus(res.JobID, res.NodeID, "COMPLETED")
	if _, ok := m.JobOutputs[res.JobID]; !ok { m.JobOutputs[res.JobID] = make(map[string]string) }
	m.JobOutputs[res.JobID][res.NodeID] = res.Result
	utils.LogJSON("INFO", "Tarea completada", map[string]interface{}{"node": res.NodeID})
	m.SaveState()

	if job, ok := m.Jobs[res.JobID]; ok {
		m.CheckAndScheduleDependents(job)
		m.CheckJobCompletion(job)
	}
	w.WriteHeader(http.StatusOK)
}