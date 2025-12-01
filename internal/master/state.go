package master

import (
	"encoding/json"
	"mini-spark/internal/common"
	"mini-spark/internal/utils"
	"os"
	"sync"
)

type Master struct {
	Workers map[string]*common.WorkerInfo
	Jobs    map[string]*common.Job

	JobProgress map[string]map[string]string
	JobOutputs  map[string]map[string]string
	JobFailures map[string]int

	TaskQueue       chan common.Task
	TaskAssignments map[string]string
	RunningTasks    map[string]common.Task

	WorkerKeys []string
	rrIndex    int
	mu         sync.Mutex
	
	stateFile string
}

func NewMaster(stateFile string) *Master {
	return &Master{
		Workers:         make(map[string]*common.WorkerInfo),
		Jobs:            make(map[string]*common.Job),
		JobProgress:     make(map[string]map[string]string),
		JobOutputs:      make(map[string]map[string]string),
		JobFailures:     make(map[string]int),
		TaskQueue:       make(chan common.Task, 100),
		TaskAssignments: make(map[string]string),
		RunningTasks:    make(map[string]common.Task),
		stateFile:       stateFile,
	}
}

// initJobProgress inicializa los mapas de estado para un nuevo job
func (m *Master) InitJobProgress(job *common.Job) {
	if _, ok := m.JobProgress[job.ID]; !ok {
		m.JobProgress[job.ID] = make(map[string]string)
	}
	if _, ok := m.JobOutputs[job.ID]; !ok {
		m.JobOutputs[job.ID] = make(map[string]string)
	}
	if _, ok := m.JobFailures[job.ID]; !ok {
		m.JobFailures[job.ID] = 0
	}
	for _, node := range job.Graph.Nodes {
		m.JobProgress[job.ID][node.ID] = "PENDING"
	}
}

func (m *Master) getNodeStatus(jobID, nodeID string) string {
	if s, ok := m.JobProgress[jobID]; ok {
		return s[nodeID]
	}
	return "PENDING"
}

func (m *Master) setNodeStatus(jobID, nodeID, status string) {
	if _, ok := m.JobProgress[jobID]; !ok {
		m.JobProgress[jobID] = make(map[string]string)
	}
	m.JobProgress[jobID][nodeID] = status
}

func (m *Master) SaveState() {
	data := struct {
		Jobs        map[string]*common.Job
		JobOutputs  map[string]map[string]string
		JobFailures map[string]int
	}{
		Jobs:        m.Jobs,
		JobOutputs:  m.JobOutputs,
		JobFailures: m.JobFailures,
	}

	file, err := os.Create(m.stateFile)
	if err != nil {
		utils.LogJSON("ERROR", "No se pudo guardar estado", map[string]interface{}{"error": err.Error()})
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		utils.LogJSON("ERROR", "Error serializando estado", map[string]interface{}{"error": err.Error()})
	}
}

func (m *Master) LoadState() {
	file, err := os.Open(m.stateFile)
	if err != nil {
		utils.LogJSON("INFO", "Iniciando sin estado previo", nil)
		return
	}
	defer file.Close()

	data := struct {
		Jobs        map[string]*common.Job
		JobOutputs  map[string]map[string]string
		JobFailures map[string]int
	}{}

	if err := json.NewDecoder(file).Decode(&data); err != nil {
		utils.LogJSON("ERROR", "Archivo de estado corrupto", map[string]interface{}{"error": err.Error()})
		return
	}

	m.Jobs = data.Jobs
	m.JobOutputs = data.JobOutputs
	m.JobFailures = data.JobFailures

	for _, job := range m.Jobs {
		m.InitJobProgress(job)
		if job.Status == "COMPLETED" {
			for _, node := range job.Graph.Nodes {
				m.setNodeStatus(job.ID, node.ID, "COMPLETED")
			}
		}
	}
	utils.LogJSON("INFO", "Estado recuperado", map[string]interface{}{"jobs_loaded": len(m.Jobs)})
}