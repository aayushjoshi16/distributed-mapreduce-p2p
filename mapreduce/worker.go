package mapreduce

import (
	"lab4/wc"
	"hash/fnv"
	"fmt"
	"log"
)

// for sorting by key.
type ByKey []wc.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func executeMTask(fileName string, mapTaskID int, nReduce int, mapf func(string, string) []wc.KeyValue) {
	

func MakeWorker(mapf func(string, string) []wc.KeyValue, reducef func(string, []string) string) {
	for {
		reply, err := CallGetTask()
		if err != nil {
			log.Fatal(err)
		}
		if reply.TaskType == MapPhase {
			executeMTask(reply.FileName, reply.MapTaskID, reply.NReduce, mapf)
			CallUpdateTaskStatus(MapPhase, reply.FileName)
		} else {
			executeRTask(reply.ReduceTaskID, reducef)
			CallUpdateTaskStatus(ReducePhasee, reply.FileName)
		}
	}
}

func CallGetTask() (*GetTaskReply, error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("MasterTask.GetTask", &args, &reply)
	if ok {
		fmt.Printf("reply.Name '%v', reply.Type '%v'\n", reply.Name, reply.Type)
		return &reply, nil
	} else {
		return nil, errors.New("call failed")
	}
}