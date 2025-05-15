package mapreduce

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type KeyValue struct {
	Key   string
	Value string
}

const (
	idle       = 0
	inprogress = 1
	completed  = 2

	BUFER_SIZE = 10240 // 10kB
)

func SplitFile(filename string) ([]byte, error) {
	// code inspired from Medium article regarding
	f, err := os.Open(filename)

	if err != nil {
		fmt.Println("Err opening file:", err)
		return []byte{}, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	fmt.Println("Splitting file:", filename)

	var buffer []byte

	for {
		temp := make([]byte, BUFER_SIZE)
		n, err := reader.Read(temp)

		buffer = append(buffer, temp[:n]...)

		//fmt.Println("Read bytes:", n)
		//fmt.Println("buffer size:", len(buffer))
		
		if n == 0 {

			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Println("Error reading file:", err)
				break
			}
			return buffer, nil

		}
	}
	return buffer, nil
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
