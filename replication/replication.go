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
	SenderId 	int			// Node that is sending the data
	StartDataId int
	EndDataId   int
	Data     	[]DataItem
}

// Struct to track dat replication task
type DataReplicationTask struct {
	Timestamp 	time.Time
	// SenderId 	int			// Node that is sending the data
	NodeId      int				// Node that is outdated and receiving the data
	StartDataId int
	EndDataId   int
}


// Function used by the leader to assign data for replication to other nodes
func (d *DataReplicationState) AssignData(server *rpc.Client, args DataReplicationRequest) error {
	fmt.Printf("Node %d: Assigning data replication tasks for node %d (last data ID: %d)\n", 
		d.NodeId, args.SenderId, args.LastDataId)

	// We need to determine how many data items we're distributing
	// Assuming the current node (leader) has the most up-to-date data
	if d.DataId <= args.LastDataId {
		fmt.Printf("No new data to replicate. Current data ID: %d, Requested last data ID: %d\n", 
			d.DataId, args.LastDataId)
		return nil
	}

	totalItems := d.DataId - args.LastDataId
	fmt.Printf("Total items to replicate: %d\n", totalItems)

	// Determine nodes to use (excluding the leader and the outdated node)
	var availableNodes []int
	for i := 1; i <= 8; i++ {
		if i != d.NodeId && i != args.SenderId {
			availableNodes = append(availableNodes, i)
		}
	}

	// Use up to 6 nodes for replication tasks
	numNodes := len(availableNodes)
	if numNodes > 6 {
		numNodes = 6
		availableNodes = availableNodes[:6]
	}

	if numNodes == 0 {
		fmt.Printf("No available nodes for data replication\n")
		return fmt.Errorf("no available nodes for replication")
	}

	// Distribute items across nodes
	itemsPerNode := totalItems / numNodes
	extraItems := totalItems % numNodes

	startId := args.LastDataId + 1
	for i := 0; i < numNodes; i++ {
		// Calculate the number of items this node will handle
		nodeItems := itemsPerNode
		if i < extraItems {
			nodeItems++
		}

		// If no items to assign, skip
		if nodeItems <= 0 {
			continue
		}

		endId := startId + nodeItems - 1
		
		taskArgs := DataReplicationTask{
			Timestamp:  time.Now(),
			NodeId:     args.SenderId,      // Node that will receive the data
			StartDataId: startId,
			EndDataId:   endId,
		}
		
		fmt.Printf("Assigning task to node %d: replicate data from %d to %d for node %d\n", 
			availableNodes[i], startId, endId, args.SenderId)
		
		// Send task to the node
		shared.SendMessage(server, availableNodes[i], taskArgs)
		
		// Update startId for the next node
		startId = endId + 1
	}

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

	// Extract data from the file based on the last data ID
	data, err := ReadDataFile(args.LastDataId + 1, args.LastDataId + 1)
	if err != nil {
		fmt.Printf("Error reading data file: %v\n", err)
	} else if dataSlice, ok := data.([]string); !ok || len(dataSlice) == 0 {
		fmt.Printf("No new data remaining to broadcast; last data ID %d\n", args.LastDataId)
		return nil
	}

	// Convert data from any type to []DataItem
	dataItems := []DataItem{{Id: args.LastDataId + 1, Value: data.([]string)[0]}}
	reply.Data = dataItems
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

	// Add this data entry to the local DataItems
	dataEntry := DataItem{
		Id:    args.LastDataId + 1,
		Value: data.([]string)[0],
	}
	d.insertDataItemSorted(dataEntry)

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
func (d *DataReplicationState) SendData(server *rpc.Client, args DataReplicationTask) error {
	// Check if the args is not outdated
	if time.Since(args.Timestamp).Seconds() > 2.0 {
		return nil
	}
	
	reply := DataReplicationResponse{
		SenderId: d.NodeId,
		StartDataId: args.StartDataId,
		EndDataId:   args.EndDataId,
	}
	fmt.Printf("Node %d: Sending data to %d from ID %d to %d\n", d.NodeId, args.NodeId, args.StartDataId, args.EndDataId)

	// Verify data in the range of startId and endId exists in DataReplicationState
	if args.StartDataId < 1 || args.EndDataId < args.StartDataId || args.EndDataId > d.DataId {
		fmt.Printf("Invalid data range: startId %d, endId %d, DataId %d\n", args.StartDataId, args.EndDataId, d.DataId)
		return fmt.Errorf("invalid data range")
	}

	// Prepare the response with the data to be sent
	reply.Timestamp = time.Now()
	data, err := ReadDataFile(args.StartDataId, args.EndDataId)
	if err != nil {
		fmt.Printf("Error reading data file: %v\n", err)
		return err
	}
	if dataSlice, ok := data.([]string); ok && len(dataSlice) > 0 {
		reply.Data = make([]DataItem, len(dataSlice))
		for i, line := range dataSlice {
			reply.Data[i] = DataItem{
				Id:    args.StartDataId + i,
				Value: line,
			}
		}
	} else {
		fmt.Printf("No data found in the specified range: startId %d, endId %d\n", args.StartDataId, args.EndDataId)
		return fmt.Errorf("no data found in the specified range")
	}

	// Send the data to the node
	if args.NodeId != d.NodeId {
		shared.SendMessage(server, args.NodeId, reply)
	} else {
		fmt.Printf("Skipping sending data to self (node %d)\n", d.NodeId)
	}

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
		if args.EndDataId > d.DataId {
			d.DataId = args.EndDataId
		}

		// Handle the received data items
		if len(args.Data) > 0 {
			dataEntry := args.Data[0]
			fmt.Printf("Data entry received: ID %d, Value: %s\n", dataEntry.Id, dataEntry.Value)

			// Insert the entry in sorted order
			d.insertDataItemSorted(dataEntry)
			// d.PrintDataItems()
		} else {
			fmt.Printf("Warning: Received empty or invalid data format\n")
		}
	} else {
		fmt.Printf("Received data from node %d for ID range %d to %d\n", args.SenderId, args.StartDataId, args.EndDataId)

		// Handle the received data items
		if len(args.Data) > 0 {
			for _, dataEntry := range args.Data {
				fmt.Printf("Data entry received: ID %d, Value: %s\n", dataEntry.Id, dataEntry.Value)
				// Insert the entry in sorted order
				d.insertDataItemSorted(dataEntry)
			}
		} else {
			fmt.Printf("Warning: Received empty or invalid data format\n")
			return fmt.Errorf("received empty or invalid data format")
		}

		fmt.Printf("Data received successfully from ID %d to %d\n", args.StartDataId, args.EndDataId)
		// Update the DataId to the last received ID
		if args.EndDataId > d.DataId {
			d.DataId = args.EndDataId
		}
		d.PrintDataItems()
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
		// Case where entry with ID already exists
		if item.Id == dataEntry.Id {
			d.DataItems[i] = dataEntry
			return
		}
		if item.Id > dataEntry.Id {
			insertIndex = i
			break
		}
		// If we reached the end without finding a larger ID, append to the end
		if i == len(d.DataItems) - 1 {
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
