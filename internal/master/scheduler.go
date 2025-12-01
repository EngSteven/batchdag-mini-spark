package master

import (
	"bytes"
	"encoding/json"
	"mini-spark/internal/common"
	"mini-spark/internal/utils"
	"net/http"
	"time"

	"github.com/google/uuid"
)

func (m *Master) ScheduleSourceTasks(job *common.Job) {
	inDegree := make(map[string]int)
	for _, node := range job.Graph.Nodes { inDegree[node.ID] = 0 }
	for _, edge := range job.Graph.Edges { inDegree[edge[1]]++ }

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, node := range job.Graph.Nodes {
		if inDegree[node.ID] == 0 { m.queueTask(job.ID, node, []string{}) }
	}
}

func (m *Master) queueTask(jobID string, node common.DAGNode, inputs []string) {
	m.setNodeStatus(jobID, node.ID, "SCHEDULED")
	task := common.Task{
		ID: uuid.New().String(), JobID: jobID, NodeID: node.ID, Op: node.Op, Fn: node.Fn,
		Args: []string{node.Path}, InputFiles: inputs, Attempt: 1,
	}
	m.TaskQueue <- task
	utils.LogJSON("INFO", "Tarea encolada", map[string]interface{}{"task_id": task.ID, "node": node.ID})
}

func (m *Master) SchedulerLoop() {
	for task := range m.TaskQueue {
		m.mu.Lock()
		var availableWorkers []*common.WorkerInfo
		for _, w := range m.Workers { if w.Status == "UP" { availableWorkers = append(availableWorkers, w) } }

		if len(availableWorkers) == 0 {
			m.mu.Unlock(); time.Sleep(2 * time.Second); m.TaskQueue <- task; continue
		}

		worker := availableWorkers[m.rrIndex%len(availableWorkers)]
		m.rrIndex++
		m.TaskAssignments[task.ID] = worker.ID
		m.RunningTasks[task.ID] = task
		m.mu.Unlock()
		go m.sendTask(worker, task)
	}
}

func (m *Master) sendTask(worker *common.WorkerInfo, task common.Task) {
	data, _ := json.Marshal(task)
	resp, err := http.Post(worker.URL+"/task", "application/json", bytes.NewBuffer(data))
	if err != nil {
		m.mu.Lock(); delete(m.TaskAssignments, task.ID); m.mu.Unlock()
		m.TaskQueue <- task; return
	}
	defer resp.Body.Close()
}

func (m *Master) CheckAndScheduleDependents(job *common.Job) {
	for _, node := range job.Graph.Nodes {
		if m.getNodeStatus(job.ID, node.ID) != "PENDING" { continue }
		allParentsDone := true
		hasParents := false
		var inputFiles []string
		for _, edge := range job.Graph.Edges {
			if edge[1] == node.ID {
				hasParents = true
				if m.getNodeStatus(job.ID, edge[0]) != "COMPLETED" { allParentsDone = false; break }
				inputFiles = append(inputFiles, m.JobOutputs[job.ID][edge[0]])
			}
		}
		if hasParents && allParentsDone { m.queueTask(job.ID, node, inputFiles) }
	}
}

func (m *Master) CheckJobCompletion(job *common.Job) {
	allDone := true
	for _, node := range job.Graph.Nodes {
		if m.getNodeStatus(job.ID, node.ID) != "COMPLETED" { allDone = false; break }
	}
	if allDone {
		utils.LogJSON("INFO", "Job completado", map[string]interface{}{"job_id": job.ID})
		job.Status = "COMPLETED"
		job.Completed = time.Now()
		m.SaveState()
	}
}

func (m *Master) HealthCheckLoop() {
	for {
		time.Sleep(5 * time.Second)
		m.mu.Lock()
		now := time.Now()
		for wID, w := range m.Workers {
			if w.Status == "UP" && now.Sub(w.LastHeartbeat) > 10*time.Second {
				utils.LogJSON("ALERT", "Worker muerto", map[string]interface{}{"worker_id": wID})
				w.Status = "DOWN"
				for tID, workerAssigned := range m.TaskAssignments {
					if workerAssigned == wID {
						if task, ok := m.RunningTasks[tID]; ok {
							delete(m.TaskAssignments, tID)
							task.ID = uuid.New().String()
							delete(m.RunningTasks, tID)
							go func(t common.Task) { m.TaskQueue <- t }(task)
						}
					}
				}
			}
		}
		m.mu.Unlock()
	}
}