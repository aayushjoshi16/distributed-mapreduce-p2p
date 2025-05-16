package mapreduce

import (
	"sync"
	"time"
	"errors"
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

type GetTaskArgs struct{}

type GetTaskReply struct {
	TaskType     TaskPhase
	FileName     string
	MapTaskID    int
	ReduceTaskID int
	NReduce      int
}

type ReportTaskArgs struct {
	TaskType TaskPhase
	TaskID   int
}
type ReportTaskReply struct{}

type MasterTask struct {
	files   []string
	nReduce int // number of reduce taks

	mu   sync.Mutex
	cond *sync.Cond

	phase           TaskPhase
	mapStatus       []TaskStatus
	reduceStatus    []TaskStatus
	mapStartTime    []time.Time
	reduceStartTime []time.Time
}

func MakeMaster(files []string, nReduce int) *MasterTask {
	m := &MasterTask{
		files:           files,
		nReduce:         nReduce,
		phase:           MapPhase,
		mapStatus:       make([]TaskStatus, len(files)),
		reduceStatus:    make([]TaskStatus, nReduce),
		mapStartTime:    make([]time.Time, len(files)),
		reduceStartTime: make([]time.Time, nReduce),
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

func (m *MasterTask) GetTask(args GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.phase == DonePhase {
		return errors.New("all tasks are done")
	}

	for i, status := range m.mapStatus {
		if status == Idle ||
			(status == InProgress && time.Since(m.mapStartTime[i]) > 10*time.Second) {
			m.mapStatus[i] = InProgress
			m.mapStartTime[i] = time.Now()

			*reply = GetTaskReply{
				TaskType:    MapPhase,
				FileName:    m.files[i],
				MapTaskID:   i,
				NReduce: m.nReduce,
			}
			return nil
		}
	
	}
	allDone := true

	for _, status := range m.mapStatus {
		if status != Completed {
			allDone = false
		}
	}
	if allDone && m.phase == MapPhase {
		m.phase = ReducePhase
	}
	
	if m.phase == DonePhase {
		*reply = GetTask
	}

}