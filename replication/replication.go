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
	NodeId     			int
	DataId     			int
	BroadcastFlag  		bool
	CheckAndDumpFlag	bool
	DataItems  			[]DataItem
}

// Struct to request lost data
type DataReplicationRequest struct {
	Timestamp 	time.Time
	Term 		int
	SenderId 	int
	StartDataId int
	EndDataId	int
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
	fmt.Printf("Node %d: Assigning data replication tasks for node %d; range: [%d, %d]\n", 
		d.NodeId, args.SenderId, args.StartDataId, args.EndDataId)

	// We need to determine how many data items we're distributing
	// Assuming the current node (leader) has the most up-to-date data
	if d.DataId <= args.EndDataId {
		fmt.Printf("No new data to replicate. Current data ID: %d, Requested last data ID: %d\n", 
			d.DataId, args.EndDataId)
		return nil
	}

	startId := 0
	totalItems := 0
	if args.EndDataId < 0 && args.StartDataId < 0 {
		// Replicate everything from the beginning
		totalItems = len(d.DataItems)
		startId = 1
	} else if args.EndDataId < 0 && args.StartDataId > 0 {
		// Replicate from the startId to the current DataId
		totalItems = len(d.DataItems)
		startId = args.StartDataId
	} else if args.EndDataId > 0 && args.StartDataId > 0 {
		// Replicate from the startId to the endId
		totalItems = args.EndDataId - args.StartDataId + 1
		if totalItems < 0 || totalItems > len(d.DataItems) {
			fmt.Printf("Invalid data range provided: StartDataId %d, EndDataId %d\n",
				args.StartDataId, args.EndDataId)
			return fmt.Errorf("invalid data range")
		}
		startId = args.StartDataId
	} else {
		// Invalid range provided
		fmt.Printf("Invalid data range provided: StartDataId %d, EndDataId %d\n", 
			args.StartDataId, args.EndDataId)
		return fmt.Errorf("invalid data range")
	}
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

// Function to broadcast data updates to all nodes to keep populating data to replicate in the system
func (d *DataReplicationState) BroadcastData(server *rpc.Client, args DataReplicationRequest) error {
	reply := DataReplicationResponse{
		SenderId:  d.NodeId,
		Data:	  nil,
		StartDataId: -1,					// -1 to indicate it's a broadcast
		EndDataId:   args.EndDataId + 1,	// Broadcasting next (single) data entry
	}

	// Extract data from the file based on the last data ID
	data, err := ReadDataFile(args.EndDataId + 1, args.EndDataId + 1)
	if err != nil {
		fmt.Printf("Error reading data file: %v\n", err)
	} else if dataSlice, ok := data.([]string); !ok || len(dataSlice) == 0 {
		fmt.Printf("No new data remaining to broadcast; last data ID %d\n", args.EndDataId)
		return nil
	}

	// Convert data from any type to []DataItem
	dataItems := []DataItem{{Id: args.EndDataId + 1, Value: data.([]string)[0]}}
	reply.Data = dataItems
	reply.Timestamp = time.Now()
	fmt.Printf("Broadcasting data %d: %v\n", args.EndDataId + 1, data)

	// Update DataId on the leader state
	d.DataId = args.EndDataId + 1

	// Send the data to all nodes
	for i := 1; i <= 8; i++ {
		if i != d.NodeId {
			shared.SendMessage(server, i, reply)
		}
	}

	// Add this data entry to the local DataItems
	dataEntry := DataItem{
		Id:    args.EndDataId + 1,
		Value: data.([]string)[0],
	}
	d.insertDataItemSorted(dataEntry)

	// Broadcast next data entry after some time
	time.AfterFunc(2 * time.Second, func() {
		newArgs := DataReplicationRequest{
			Timestamp:  time.Now(),
			Term:       args.Term,
			SenderId:   d.NodeId,
			EndDataId: args.EndDataId + 1,
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
func (d *DataReplicationState) ReceiveData(server *rpc.Client, args DataReplicationResponse, leaderId int) error {
	// Check if the args is not outdated
	if time.Since(args.Timestamp).Seconds() > 2.0 {
		return nil
	}

	// Turn broadcast mode off if it was on
	if d.BroadcastFlag {
		d.BroadcastFlag = false
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

	// Start check and dump process if the flag is not set
	if !d.CheckAndDumpFlag {
		d.CheckAndDumpFlag = true
		go d.CheckAndDump(server, leaderId)
	}

	return nil
}

// Function that checks for missing data and writes to local disk
func (d *DataReplicationState) CheckAndDump(server *rpc.Client, leaderId int) error {
	// Check in-memory data items to check for missing data
	fmt.Printf("Node %d: Checking for missing data...\n", d.NodeId)

	missingRanges := make([][2]int, 0)
	
	// First check in-memory data for gaps
	if len(d.DataItems) > 0 {
		lastSeenId := 0
		
		for _, item := range d.DataItems {
			// If there's a gap between the last seen ID and current item ID
			if item.Id > lastSeenId + 1 {
				missingRanges = append(missingRanges, [2]int{lastSeenId + 1, item.Id - 1})
			}
			lastSeenId = item.Id
		}
		
		// Check if there are missing items after the last in-memory item up to DataId
		if lastSeenId < d.DataId {
			missingRanges = append(missingRanges, [2]int{lastSeenId + 1, d.DataId})
		}
	}

	// Check if the file exists and read the last ID from it
	fileName := fmt.Sprintf("%d-replication", d.NodeId)
	lastFileId := 0
	
	// Try to read the last ID from the file
	file, err := os.Open(fileName)
	if err == nil {
		defer file.Close()
		
		// Scan through the file to get the last line
		scanner := bufio.NewScanner(file)
		var lastLine string
		
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {
				lastLine = line
			}
		}
		
		// Parse the last line to get the ID
		if lastLine != "" {
			var id int
			_, err := fmt.Sscanf(lastLine, "ID: %d,", &id)
			if err == nil {
				lastFileId = id
				fmt.Printf("Node %d: Last ID in file: %d\n", d.NodeId, lastFileId)
			} else {
				fmt.Printf("Node %d: Error parsing last line ID: %v\n", d.NodeId, err)
			}
		}
		
		if err := scanner.Err(); err != nil {
			fmt.Printf("Node %d: Error reading file: %v\n", d.NodeId, err)
		}
	} else if !os.IsNotExist(err) {
		// Only report error if it's not a "file doesn't exist" error
		fmt.Printf("Node %d: Error opening file: %v\n", d.NodeId, err)
	}
	
	// Check for gap between file and in-memory data
	if len(d.DataItems) > 0 && lastFileId > 0 {
		firstMemoryId := d.DataItems[0].Id
		
		// If there's a gap between file and memory, add it to missing ranges
		if firstMemoryId > lastFileId + 1 {
			fmt.Printf("Node %d: Gap detected between file (last ID: %d) and memory (first ID: %d)\n", 
				d.NodeId, lastFileId, firstMemoryId)
			missingRanges = append(missingRanges, [2]int{lastFileId + 1, firstMemoryId - 1})
		}
	}
	
	// Handle missing ranges if any are found
	if len(missingRanges) > 0 {
		for _, r := range missingRanges {
			fmt.Printf("Node %d: Missing data range from %d to %d\n", d.NodeId, r[0], r[1])
			
			// Request data replication for the missing range
			args := DataReplicationRequest{
				Timestamp:   time.Now(),
				SenderId:    d.NodeId,
				StartDataId: r[0],
				EndDataId:   r[1],
			}
			shared.SendMessage(server, leaderId, args)
		}
	} else {
		// No missing ranges found, safe to dump data if we have enough items
		if len(d.DataItems) >= 5 {
			if err := d.dumpDataToFile(); err != nil {
				fmt.Printf("Node %d: Error dumping data: %v\n", d.NodeId, err)
			} else {
				// Clear in-memory data after successful dump
				d.DataItems = []DataItem{}
			}
		}
	}
	
	// Schedule next check
	time.AfterFunc(10 * time.Second, func() {
		d.CheckAndDump(server, leaderId)
	})
	
	return nil
}

// Helper method to dump data to file
func (d *DataReplicationState) dumpDataToFile() error {
	fileName := fmt.Sprintf("%d-replication", d.NodeId)
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", fileName, err)
	}
	defer file.Close()
	
	// Write data items to file
	for _, item := range d.DataItems {
		// Write both ID and value to keep track of IDs in the file
		line := fmt.Sprintf("ID: %d, Value: %s\n", item.Id, item.Value)
		if _, err := file.WriteString(line); err != nil {
			return fmt.Errorf("failed to write to file %s: %v", fileName, err)
		}
	}
	
	fmt.Printf("Node %d: Data dumped successfully to file %s (%d items)\n", 
		d.NodeId, fileName, len(d.DataItems))
	return nil
}

// Function to read data from file
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
	fmt.Printf("-------------------------------\n")
	for _, item := range d.DataItems {
		fmt.Printf("ID: %d, Value: %s\n", item.Id, item.Value)
	}
	fmt.Printf("-------------------------------\n")
}
