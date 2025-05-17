package mapreduce

import (
	"errors"
	"fmt"
	"sync"
	"time"
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
	NMap         int
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
		*reply = GetTaskReply{
			TaskType: DonePhase,
			NMap:     len(m.files),
		}
		return nil
	}

	if m.phase == MapPhase {
		for i, status := range m.mapStatus {
			if status == Idle ||
				(status == InProgress && time.Since(m.mapStartTime[i]) > 10*time.Second) {
				m.mapStatus[i] = InProgress
				m.mapStartTime[i] = time.Now()

				if i < len(m.files) {
					*reply = GetTaskReply{
						TaskType:  MapPhase,
						FileName:  m.files[i],
						MapTaskID: i,
						NReduce:   m.nReduce,
						NMap:      len(m.files),
					}
					fmt.Printf("Assigning map task %d with file %s\n", i, m.files[i])
					return nil
				} else {
					fmt.Printf("Warning: Attempted to access file index %d, but only have %d files\n",
						i, len(m.files))
					m.mapStatus[i] = Idle // Reset status since we can't process it
					continue
				}
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
	}

	if m.phase == ReducePhase {
		for i, status := range m.reduceStatus {
			if status == Idle ||
				(status == InProgress && time.Since(m.reduceStartTime[i]) > 10*time.Second) {
				m.reduceStatus[i] = InProgress
				m.reduceStartTime[i] = time.Now()

				*reply = GetTaskReply{
					TaskType:     ReducePhase,
					ReduceTaskID: i,
					NReduce:      m.nReduce,
				}
				return nil
			}

		}

		if m.phase == DonePhase {
			*reply = GetTaskReply{
				TaskType: DonePhase,
			}
			return nil
		}
	}
	return errors.New("no tasks available")
}

func (m *MasterTask) ReportTaskDone(args ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch args.TaskType {
	case MapPhase:
		if args.TaskID < 0 || args.TaskID >= len(m.mapStatus) {
			return errors.New("invalid MapTaskID")
		}
		m.mapStatus[args.TaskID] = Completed

		all := true
		for _, s := range m.mapStatus {
			if s != Completed {
				all = false
				break
			}
		}
		if all {
			m.phase = ReducePhase
		}

	case ReducePhase:
		if args.TaskID < 0 || args.TaskID >= len(m.reduceStatus) {
			return errors.New("invalid ReduceTaskID")
		}
		m.reduceStatus[args.TaskID] = Completed

		all := true
		for _, s := range m.reduceStatus {
			if s != Completed {
				all = false
				break
			}
		}
		if all {
			m.phase = DonePhase
		}
	default:
		return errors.New("unknown TaskType")
	}
	return nil
}

func (m *MasterTask) HandleWorkerFailure(workerID int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, status := range m.mapStatus {
		if status == InProgress && time.Since(m.mapStartTime[i]) > 10*time.Second {
			m.mapStatus[i] = Idle
		}
	}

	for i, status := range m.reduceStatus {
		if status == InProgress && time.Since(m.reduceStartTime[i]) > 10*time.Second {
			m.reduceStatus[i] = Idle
		}
	}

}
