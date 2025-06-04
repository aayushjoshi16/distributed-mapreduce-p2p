package replication

import (
	"bufio"
	"fmt"
	"lab4/shared"
	"net/rpc"
	"os"
	"time"
)

type DataItem struct {
	Id    int
	Value string
}

type DataReplicationState struct {
	NodeId     int
	DataId     int
	Broadcast  bool
	DataItems  []DataItem
}

// Struct to request lost data
type DataReplicationRequest struct {
	Timestamp 	time.Time
	Term 		int
	SenderId 	int
	LastDataId  int
}

// Struct to return data
type DataReplicationResponse struct {
	Timestamp 	time.Time
	SenderId 	int
	StartDataId int
	EndDataId   int
	Data     	any
}

// Struct to track dat replication task
type DataReplicationTask struct {
	Timestamp 	time.Time
	SenderId 	int
	StartDataId int
	EndDataId   int
}


// Function used by the leader to assign data for replication to other nodes
func (d *DataReplicationState) AssignData(args DataReplicationRequest, reply DataReplicationResponse) error {
	fmt.Printf("Testing function AssignData call")
	return nil
}

// Function to broadcast updates to all nodes
func (d *DataReplicationState) BroadcastData(server *rpc.Client, args DataReplicationRequest) error {
	reply := DataReplicationResponse{
		SenderId:  d.NodeId,
		Data:	  nil,
		StartDataId: -1,					// -1 to indicate it's a broadcast
		EndDataId:   args.LastDataId + 1,	// Broadcasting next (single) data entry
	}
	// reply.StartDataId = -1
	// reply.EndDataId = args.LastDataId + 1
	// reply.SenderId = d.NodeId

	// Extract data from the file based on the last data ID
	data, err := ReadDataFile(args.LastDataId + 1, args.LastDataId + 1)
	if err != nil {
		fmt.Printf("Error reading data file: %v\n", err)
	} else if dataSlice, ok := data.([]string); !ok || len(dataSlice) == 0 {
		fmt.Printf("No new data remaining to broadcast; last data ID %d\n", args.LastDataId)
		return nil
	}

	reply.Data = data
	reply.Timestamp = time.Now()
	fmt.Printf("Broadcasting data %d: %v\n", args.LastDataId + 1, data)

	// Update DataId on the leader state
	d.DataId = args.LastDataId + 1

	// Send the data to all nodes
	for i := 1; i <= 8; i++ {
		if i != d.NodeId {
			shared.SendMessage(server, i, reply)
		}
	}

	// Broadcast next data entry after 5 seconds
	time.AfterFunc(5*time.Second, func() {
		newArgs := DataReplicationRequest{
			Timestamp:  time.Now(),
			Term:       args.Term,
			SenderId:   d.NodeId,
			LastDataId: args.LastDataId + 1,
		}
		d.BroadcastData(server, newArgs)
	})

	return nil
}

// Function to send data to all nodes
func (d *DataReplicationState) SendData(args DataReplicationRequest, reply DataReplicationResponse) error {
	fmt.Printf("Sending data from ID %d to %d\n", args.LastDataId+1, d.DataId)

	reply.StartDataId = args.LastDataId + 1
	// reply.EndDataId = d.DataID
	reply.EndDataId = 5
	reply.SenderId = d.NodeId

	data, err := ReadDataFile(reply.StartDataId, reply.EndDataId)
	if err != nil {
		fmt.Printf("Error reading data file: %v\n", err)
	}

	reply.Data = data
	reply.Timestamp = time.Now()
	fmt.Printf("Data sent successfully from ID %d to %d\n", reply.StartDataId, reply.EndDataId)

	return nil
}

// Function to receive data from other nodes
func (d *DataReplicationState) ReceiveData(args DataReplicationResponse) error {
	// Check if the args is not outdated
	if time.Since(args.Timestamp).Seconds() > 2.0 {
		return nil
	}

	// Turn broadcast mode off if it was on
	if d.Broadcast {
		d.Broadcast = false
	}

	// Update local state with the received data for broadcast
	if args.StartDataId == -1 {
		fmt.Printf("Node %d: Received broadcast data from node %d\n", d.NodeId, args.SenderId)
		d.DataId = args.EndDataId

		// Handle the case where data is a slice of strings
		if dataSlice, ok := args.Data.([]string); ok && len(dataSlice) > 0 {
			dataEntry := DataItem{
				Id:    args.EndDataId,
				Value: dataSlice[0], // Take the first string from the slice
			}

			fmt.Printf("Data entry received: ID %d, Value: %s\n", dataEntry.Id, dataEntry.Value)

			// Insert the entry in sorted order
			d.insertDataItemSorted(dataEntry)
			d.PrintDataItems()
		} else {
			fmt.Printf("Warning: Received empty or invalid data format\n")
		}
	} else {
		fmt.Printf("Received data from node %d for ID range %d to %d\n", args.SenderId, args.StartDataId, args.EndDataId)
		if d.DataId >= args.EndDataId {
			fmt.Printf("No new data to append for ID range %d to %d\n", args.StartDataId, args.EndDataId)
			return nil
		}
		d.DataId = args.EndDataId
	}

	// Append write the received data to the file named "<d.NodeId>-replication"
	// fileName := fmt.Sprintf("%d-replication", d.NodeId)
	// file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// if err != nil {
	// 	return fmt.Errorf("failed to open file %s: %v", fileName, err)
	// }
	// defer file.Close()
	// // Write the data to the file
	// for _, line := range args.Data.([]string) {
	// 	if _, err := file.WriteString(line + "\n"); err != nil {
	// 		return fmt.Errorf("failed to write to file %s: %v", fileName, err)
	// 	}
	// }
	// fmt.Printf("Data received successfully from ID %d to %d\n", args.StartDataId, args.EndDataId)
	return nil
}

// Function that reads file and returns data
func ReadDataFile(startId int, endId int) (any, error) {
	// Open the file
	file, err := os.Open("mr-out-final")
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()
	
	// Read the file line by line
	scanner := bufio.NewScanner(file)
	lineNum := 1
	var data []string
	
	// Collect lines from startId to endId
	for scanner.Scan() {
		if lineNum >= startId && lineNum <= endId {
			line := scanner.Text()
			data = append(data, line)
		} else if lineNum > endId {
			break
		}
		lineNum++
	}
	
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}
	
	return data, nil
}

// Function to insert a DataItem into DataItems in sorted order based on Id
func (d *DataReplicationState) insertDataItemSorted(dataEntry DataItem) {
	insertIndex := 0
	for i, item := range d.DataItems {
		if item.Id > dataEntry.Id {
			insertIndex = i
			break
		}
		if i == len(d.DataItems)-1 {
			// If we reached the end without finding a larger Id, append to the end
			insertIndex = len(d.DataItems)
		}
	}
	
	// Special case for empty slice or when the new item should be at the end
	if len(d.DataItems) == 0 || insertIndex == len(d.DataItems) {
		d.DataItems = append(d.DataItems, dataEntry)
	} else {
		// Insert at the correct position
		d.DataItems = append(d.DataItems[:insertIndex+1], d.DataItems[insertIndex:]...)
		d.DataItems[insertIndex] = dataEntry
	}
}

// Function to print DataItems array
func (d *DataReplicationState) PrintDataItems() {
	for _, item := range d.DataItems {
		fmt.Printf("ID: %d, Value: %s\n", item.Id, item.Value)
	}
}
