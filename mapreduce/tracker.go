package mapreduce

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	ch "lab4/chunks"
	"os"
	"sync"
	"time"
)

const TASK_TIMEOUT = 10 * time.Second

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
	MapPhase:    "map",
	ReducePhase: "reduce",
	DonePhase:   "done",
}

var strToTaskPhase = invertMap(taskPhaseToStr)

func (p TaskPhase) MarshalJSON() ([]byte, error) {
	str, ok := taskPhaseToStr[p]
	if !ok {
		str = "unknown"
	}
	return json.Marshal(str)
}

func (p TaskPhase) String() string {
	str, ok := taskPhaseToStr[p]
	if !ok {
		str = "unknown"
	}
	return str
}

func (p *TaskPhase) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	if val, ok := strToTaskPhase[str]; ok {
		*p = val
		return nil
	}
	return fmt.Errorf("invalid task phase: %q", str)
}

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

var taskStatusToStr = map[TaskStatus]string{
	Idle:       "idle",
	InProgress: "in_progress",
	Completed:  "completed",
}

var strToTaskStatus = invertMap(taskStatusToStr)

func (s TaskStatus) MarshalJSON() ([]byte, error) {
	str, ok := taskStatusToStr[s]
	if !ok {
		str = "unknown"
	}
	return json.Marshal(str)
}

func (s TaskStatus) String() string {
	str, ok := taskStatusToStr[s]
	if !ok {
		str = "unknown"
	}
	return str
}

func (s *TaskStatus) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	if val, ok := strToTaskStatus[str]; ok {
		*s = val
		return nil
	}
	return fmt.Errorf("invalid task status: %q", str)
}

type GetTaskArgs struct {
	SenderId int
}

type GetTaskReply struct {
	TaskType     TaskPhase
	FileName     ch.Chunk
	MapTaskID    int
	ReduceTaskID int
	NReduce      int
	NMap         int
	NoTasks      bool // really jank garbage
}

type ReportTaskArgs struct {
	TaskType TaskPhase
	TaskID   int
}

type MasterTask struct {
	files   []ch.Chunk
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
	FileName  ch.Chunk   `json:"file_name"`
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
	chunks := ch.ChunkFiles(files)
	m := &MasterTask{
		files:           chunks,
		nReduce:         nReduce,
		phase:           MapPhase,
		mapStatus:       make([]TaskStatus, len(chunks)),
		reduceStatus:    make([]TaskStatus, nReduce),
		mapStartTime:    make([]time.Time, len(chunks)),
		reduceStartTime: make([]time.Time, nReduce),
	}
	m.ApplyTaskLogs("task_log.jsonl")
	return m
}

func (m *MasterTask) ApplyTaskLogs(filename string) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		var le TaskLogEntry
		if err := json.Unmarshal(scanner.Bytes(), &le); err != nil {
			return
		}
		if le.Phase == MapPhase {
			m.mapStatus[le.TaskID] = le.Status
			if le.Status == InProgress {
				m.mapStartTime[le.TaskID] = le.Timestamp
			}
		} else if le.Phase == ReducePhase {
			m.reduceStatus[le.TaskID] = le.Status
			if le.Status == InProgress {
				m.reduceStartTime[le.TaskID] = le.Timestamp
			}
		}
		m.phase = le.Phase
	}

	fmt.Printf("Restored tracker %+v\n", m)
}

func (m *MasterTask) GetTask(args GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	time.Sleep(500 * time.Millisecond)

	if m.phase == MapPhase {
		for i, status := range m.mapStatus {
			if status == Idle || (status == InProgress && time.Now().After(m.mapStartTime[i].Add(TASK_TIMEOUT))) {
				m.mapStatus[i] = InProgress
				m.mapStartTime[i] = time.Now()

				*reply = GetTaskReply{
					TaskType:  MapPhase,
					FileName:  m.files[i],
					MapTaskID: i,
					NReduce:   m.nReduce,
					NMap:      len(m.files),
				}
				logTask(TaskLogEntry{
					Phase:     MapPhase,
					TaskID:    i,
					FileName:  m.files[i],
					Status:    InProgress,
					Timestamp: m.mapStartTime[i],
				})
				fmt.Printf("Assigning map task %d with file %s\n", i, m.files[i])
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
	} else if m.phase == ReducePhase {
		for i, status := range m.reduceStatus {
			if status == Idle || (status == InProgress && time.Now().After(m.reduceStartTime[i].Add(TASK_TIMEOUT))) {
				m.reduceStatus[i] = InProgress
				m.reduceStartTime[i] = time.Now()

				*reply = GetTaskReply{
					TaskType:     ReducePhase,
					ReduceTaskID: i,
					NMap:         len(m.files),
					NReduce:      m.nReduce,
				}
				logTask(TaskLogEntry{
					Phase:     ReducePhase,
					TaskID:    i,
					Status:    InProgress,
					Timestamp: m.reduceStartTime[i],
				})
				fmt.Printf("Assigning reduce task %d\n", i)
				return nil
			}

		}
	} else if m.phase == DonePhase {
		*reply = GetTaskReply{
			TaskType: DonePhase,
		}
		logTask(TaskLogEntry{
			Phase:     DonePhase,
			Timestamp: time.Now(),
		})
		return nil
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

		logTask(TaskLogEntry{
			Phase:     m.phase,
			TaskID:    args.TaskID,
			FileName:  m.files[args.TaskID],
			Status:    m.mapStatus[args.TaskID],
			Timestamp: time.Now(),
		})

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

		logTask(TaskLogEntry{
			Phase:     m.phase,
			TaskID:    args.TaskID,
			Status:    m.reduceStatus[args.TaskID],
			Timestamp: time.Now(),
		})

		all := true
		for _, s := range m.reduceStatus {
			if s != Completed {
				all = false
				break
			}
		}
		if all {
			//fmt.Printf("Node %d: Leader detected, merging outputs...\n", s.id)
			err := MergeReduceOutputs(m.nReduce, "mr-out-final")
			if err != nil {
				fmt.Printf("Error merging outputs: %v\n", err)
			} else {
				fmt.Printf("Successfully merged outputs to mr-out-final\n")
			}

			m.phase = DonePhase
		}
	default:
		return errors.New("unknown TaskType")
	}

	return nil
}

// func (m *MasterTask) HandleWorkerFailure(workerID int) {
// 	m.mu.Lock()
// 	defer m.mu.Unlock()

// 	for i, status := range m.mapStatus {
// 		if status == InProgress && time.Since(m.mapStartTime[i]) > 10*time.Second {
// 			m.mapStatus[i] = Idle
// 		}
// 	}

// 	for i, status := range m.reduceStatus {
// 		if status == InProgress && time.Since(m.reduceStartTime[i]) > 10*time.Second {
// 			m.reduceStatus[i] = Idle
// 		}
// 	}

// }
