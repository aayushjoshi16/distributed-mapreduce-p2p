package mapreduce

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	ch "lab4/chunks"
	"lab4/wc"
	"os"
	"sort"
	"strconv"
	"strings"
)

type DataReplication struct {
	SenderId int
	Term     int
}

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

func readChunk(filename string, start, end int64) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Seek to the start position
	_, err = file.Seek(start, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)

	// Skip until after the first newline
	var startOffset int64 = start
	if startOffset > 0 {
		for {
			b, err := reader.ReadByte()
			startOffset++
			if err != nil {
				return "", err
			}
			if b == ' ' {
				break
			}
		}
	}

	// Now read from after that newline
	var buf bytes.Buffer
	var currentOffset = startOffset

	for {
		line, err := reader.ReadBytes(' ')
		if len(line) > 0 {
			buf.Write(line)
			currentOffset += int64(len(line))
			if currentOffset > end {
				break
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err // return actual error
		}
	}

	return buf.String(), nil
}

func ExecuteMTask(fileName ch.Chunk, mapTaskID int, nReduce int, mapf func(string, string) []wc.KeyValue) error {
	fmt.Printf("Map task %d: Processing file %v\n", mapTaskID, fileName)

	// Check if file exists
	if _, err := os.Stat(fileName.Filename); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %v", fileName)
	}

	// Read input file

	content, err := readChunk(fileName.Filename, fileName.Start, fileName.End)
	if err != nil {
		return fmt.Errorf("cannot read file: %v", err)
	}

	// dump chunk to file (for debugging)
	// file, err := os.Open(fileName.Filename)
	// if err != nil {
	// 	return "", err
	// }
	// defer file.Close()

	// Apply the map function
	kva := mapf(fileName.Filename, string(content))
	fmt.Printf("Map task %d: Generated %d key-value pairs\n", mapTaskID, len(kva))

	// Create buckets for each reduce task
	buckets := make([][]wc.KeyValue, nReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	// Write intermediate files
	for r := 0; r < nReduce; r++ {
		outname := fmt.Sprintf("mr-%d-%d", mapTaskID, r)
		fmt.Printf("Map task %d: Writing intermediate file %s with %d pairs\n",
			mapTaskID, outname, len(buckets[r]))

		outfile, err := os.Create(outname)
		if err != nil {
			return fmt.Errorf("cannot create intermediate file: %v", err)
		}

		encoder := json.NewEncoder(outfile)
		for _, kv := range buckets[r] {
			if err := encoder.Encode(&kv); err != nil {
				outfile.Close()
				return fmt.Errorf("cannot encode key-value pair: %v", err)
			}
		}
		outfile.Close()
	}

	return nil
}

func ExecuteRTask(reduceTaskID int, nMap int, reducef func(string, []string) string) error {
	fmt.Printf("Reduce task %d: Starting with %d map tasks\n", reduceTaskID, nMap)

	// Create map to store key-value pairs
	intermediate := make(map[string][]string)

	// Process all intermediate files for this reduce task
	for m := 0; m < nMap; m++ {
		filename := fmt.Sprintf("mr-%d-%d", m, reduceTaskID)

		// Skip if file doesn't exist
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			fmt.Printf("Reduce task %d: File %s does not exist, skipping\n",
				reduceTaskID, filename)
			continue
		}

		fmt.Printf("Reduce task %d: Reading intermediate file %s\n",
			reduceTaskID, filename)

		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("Reduce task %d: Warning - cannot open file: %v\n",
				reduceTaskID, err)
			continue
		}

		// Decode key-value pairs
		decoder := json.NewDecoder(file)
		count := 0
		for {
			var kv wc.KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
			count++
		}
		fmt.Printf("Reduce task %d: Read %d pairs from %s\n",
			reduceTaskID, count, filename)
		file.Close()
	}

	// Sort keys
	var keys []string
	for k := range intermediate {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create output file
	outname := fmt.Sprintf("mr-out-%d", reduceTaskID)
	outfile, err := os.Create(outname)
	if err != nil {
		return fmt.Errorf("cannot create output file: %v", err)
	}

	// Apply reduce function and write results
	for _, k := range keys {
		output := reducef(k, intermediate[k])
		fmt.Fprintf(outfile, "%v %v\n", k, output)
	}
	outfile.Close()

	fmt.Printf("Reduce task %d: Created output file %s with %d entries\n",
		reduceTaskID, outname, len(keys))

	return nil
}

func MergeReduceOutputs(nReduce int, outputFile string) error {
	outfile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("cannot create merged output file: %v", err)
	}
	defer outfile.Close()

	wordCounts := make(map[string]int)

	for r := range nReduce {
		filename := fmt.Sprintf("mr-out-%d", r)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			continue
		}

		content, err := os.ReadFile(filename)

		if err != nil {
			fmt.Printf("Warning: cannot read %s: %v\n", filename, err)
			continue
		}

		for _, line := range strings.Split(string(content), "\n") {
			if len(line) == 0 {
				continue
			}

			parts := strings.Fields(line)

			if len(parts) >= 2 {
				word := parts[0]
				count, err := strconv.Atoi(parts[1])
				if err != nil {
					fmt.Printf("Warning: Invalid count format for word %s: %s\n", word, parts[1])
					continue
				}

				// Add to the existing count
				wordCounts[word] += count
			}
		}
	}

	var words []string
	for word := range wordCounts {
		words = append(words, word)
	}
	sort.Strings(words)

	for _, word := range words {
		fmt.Fprintf(outfile, "%s %d\n", word, wordCounts[word])
	}

	return nil
}

/*
func WorkerLoop (mapf func(string, string) []wc.KeyValue, reducef func(string, []string) string) {
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


func MakeWorker(mapf func(string, string) []wc.KeyValue, reducef func(string, []string) string) {
	for {
		reply, err := CallGetTask()
		if err != nil {
			log.Fatal(err)
		}
		if reply.TaskType == MapPhase {
			ExecuteMTask(reply.FileName, reply.MapTaskID, reply.NReduce, mapf)
			CallUpdateTaskStatus(MapPhase, reply.FileName)
		} else {
			ExecuteRTask(reply.ReduceTaskID, reducef)
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
*/
