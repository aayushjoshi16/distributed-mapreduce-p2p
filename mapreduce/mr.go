package mapreduce

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type KeyValue struct {
	Key   string
	Value string
}

const (
	idle       = 0
	inprogress = 1
	completed  = 2

	BUFFER_SIZE = 10240 // 10kB
)

func SplitFile(filename string) ([]string, error) {
	// code inspired from Medium article regarding
	f, err := os.Open(filename)

	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanWords)

	//fmt.Println("Splitting file:", filename)

	var chunks []string

	var buffer strings.Builder

	for scanner.Scan() {
		word := scanner.Text()

		tokenSize := len(word) + 1 // +1 for space

		if buffer.Len()+tokenSize > BUFFER_SIZE && buffer.Len() > 0 {
			chunks = append(chunks, buffer.String())
			buffer.Reset()
		}

		if buffer.Len() > 0 {
			buffer.WriteString(" ")
		}
		buffer.WriteString(word)
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return nil, err

	}
	if buffer.Len() > 0 {
		chunks = append(chunks, buffer.String())
	}
	//fmt.Println("File split into", len(chunks), "chunks")
	return chunks, nil
}

// Structure for replication
// Replicaiton betwen leader and atleast one follower
/*
1. Have the leader in a persistent state
2. after that trigger the splitting and distribution of tasks
3. Everything maps and reduces
3. After that pulling tasks, ideally shared channel to pull tasks may be impracticaly
4. Probably just have leader split tasks based on hashing and modulus
5. Write the mapping to an intermediate file
6. After that, multiple reducers


*/
