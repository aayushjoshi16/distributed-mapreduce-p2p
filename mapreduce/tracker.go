package mapreduce

import ("sync"

)

type TaskPhase int
const (
	MapPhase TaskPhase = iota
	ReducePhase
	DonePhase
)

type TaskStatus int
const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type MasterTask struct {
	files []string // list we're processing
	nReducer int
	mutex sync.Mutex
	phase TaskPhase
	status TaskStatus
	mapTasks []TaskStatus
	reduceTasks []TaskStatus

	cond *sync.Cond
}

func MakeMaster(files []string, nReducer int) *MasterTask {
	m := &MasterTask{
		files: files,
		nReducer: nReducer, // number of reducers
		phase: MapPhase,
		status: Idle,
		mapTasks: make([]TaskStatus, len(files)),
		reduceTasks: make([]TaskStatus, nReducer),
	}
	m.cond = sync.NewCond(&m.mutex)
	
	return m
}