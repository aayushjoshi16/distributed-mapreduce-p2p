package mapreduce

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

func invertMap[K comparable, V comparable](m map[K]V) map[V]K {
	inverted := make(map[V]K)
	for k, v := range m {
		inverted[v] = k
	}
	return inverted
}

type TaskPhase int

const (
	MapPhase TaskPhase = iota
	ReducePhase
	DonePhase
)

var taskPhaseToStr = map[TaskPhase]string{
	MapPhase:    "phase",
	ReducePhase: "reduce",
	DonePhase:   "done",
}

func (p TaskPhase) MarshalJSON() ([]byte, error) {
	return json.Marshal(str)
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type GetTaskArgs struct {
	SenderId int
}

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

type MasterTask struct {
	files   []string
	nReduce int // number of reduce taks

	mu sync.Mutex

	phase           TaskPhase
	mapStatus       []TaskStatus
	reduceStatus    []TaskStatus
	mapStartTime    []time.Time
	reduceStartTime []time.Time
}

// Imma level with you chief that is giving big chatgpt vibes and in the way where the code fucking sucks and makes me want to commit murder
type TaskLogEntry struct {
	Phase     TaskPhase  `json:"phase"`
	TaskID    int        `json:"task_id"`
	FileName  string     `json:"file_name,omitempty"`
	Status    TaskStatus `json:"status"` // "idle", "in_progress", "completed"
	Timestamp time.Time  `json:"timestamp"`
}

func logTask(entry TaskLogEntry) {
	file, err := os.OpenFile("task_log.jsonl", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to write log: %v\n", err)
		return
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	if err := enc.Encode(entry); err != nil {
		fmt.Printf("Failed to encode log entry: %v\n", err)
	}
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
					logTask(TaskLogEntry{
						Phase:     "map",
						TaskID:    i,
						FileName:  m.files[i],
						Status:    "in_progress",
						Timestamp: time.Now(),
					})
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
				logTask(TaskLogEntry{
					Phase:     "reduce",
					TaskID:    i,
					Status:    "in_progress",
					Timestamp: time.Now(),
				})
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

func (m *MasterTask) ReportTaskDone(args ReportTaskArgs) error {
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
