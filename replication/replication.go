package replication

import (
	"bufio"
	"encoding/json"
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
	SenderId 	int				// Node that is sending the data
	StartDataId int
	EndDataId   int
	Data     	[]DataItem
}

// Struct to track dat replication task
type DataReplicationTask struct {
	Timestamp 	time.Time
	NodeId      int				// Node that is outdated and receiving the data
	StartDataId int
	EndDataId   int
}


// Leader: Function used by the leader to assign data for replication to other nodes
func (d *DataReplicationState) AssignData(server *rpc.Client, args DataReplicationRequest) error {
	// Check if the args is not outdated
	if time.Since(args.Timestamp).Seconds() > 2.0 {
		return nil
	}

	fmt.Printf("Node %d: Assigning data replication tasks for node %d; range: [%d, %d]\n", 
		d.NodeId, args.SenderId, args.StartDataId, args.EndDataId)

	// We need to determine how many data items we're distributing
	if d.DataId <= args.EndDataId {
		fmt.Printf("No new data to replicate. Current data ID: %d, Requested last data ID: %d\n", 
			d.DataId, args.EndDataId)
		return nil
	}

	startId := 0
	totalItems := 0
	
	// Calculate range for replication
	if args.EndDataId < 0 && args.StartDataId < 0 {
		// Replicate everything from the beginning
		totalItems = 1
		// Start from 1 since DataId starts from 1
		// This way the gaps between 1 and DataId will be filled in future iterations
		startId = 1
	} else if args.EndDataId < 0 && args.StartDataId > 0 {
		// Replicate from the startId to the current DataId
		totalItems = d.DataId - args.StartDataId + 1
		startId = args.StartDataId
	} else if args.EndDataId > 0 && args.StartDataId > 0 {
		// Replicate from the startId to the endId
		totalItems = args.EndDataId - args.StartDataId + 1
		if totalItems < 0 || totalItems > d.DataId {
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
			NodeId:     args.SenderId,
			StartDataId: startId,
			EndDataId:   endId,
		}
		
		fmt.Printf("Assigning task to node %d: replicate data [%d, %d] for node %d\n", 
			availableNodes[i], startId, endId, args.SenderId)
		
		shared.SendMessage(server, availableNodes[i], taskArgs)
		startId = endId + 1
	}

	return nil
}

// Leader: Function to broadcast data updates to all nodes to keep populating data to replicate in the system
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

	// Start the leader's periodic data dump if not already started
	if !d.CheckAndDumpFlag {
		d.CheckAndDumpFlag = true
		go d.CheckAndDump(server, d.NodeId)
	}

	// Schedule next broadcast
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

// Worker: Function to send data to all nodes
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

	// First check if the data is available in memory
	if args.StartDataId >= 1 && args.EndDataId >= args.StartDataId && args.EndDataId <= d.DataId {
		// Try to get data from in-memory items first
		var inMemoryData []DataItem
		for _, item := range d.DataItems {
			if item.Id >= args.StartDataId && item.Id <= args.EndDataId {
				inMemoryData = append(inMemoryData, item)
			}
		}
		
		// If we found all requested items in memory, use them
		if len(inMemoryData) > 0 && inMemoryData[0].Id == args.StartDataId && 
		   inMemoryData[len(inMemoryData)-1].Id == args.EndDataId && 
		   len(inMemoryData) == (args.EndDataId - args.StartDataId + 1) {
			reply.Data = inMemoryData
			fmt.Printf("Node %d: Found complete data range [%d, %d] in memory\n", 
				d.NodeId, args.StartDataId, args.EndDataId)
		}
	}
	
	// Read the file if the data is not found in memory
	if len(reply.Data) == 0 {
		fmt.Printf("Node %d: Reading from file for range [%d, %d]\n", d.NodeId, args.StartDataId, args.EndDataId)
		fileData, err := d.ReadDataById(args.StartDataId, args.EndDataId)
		if err == nil && len(fileData) > 0 {
			reply.Data = fileData
			fmt.Printf("Node %d: Found %d items in file\n", d.NodeId, len(fileData))
		} else {
			return fmt.Errorf("no data found in the specified range: startId %d, endId %d", args.StartDataId, args.EndDataId)
		}
	}
	
	// Return an error if no data was found
	if len(reply.Data) == 0 {
		return fmt.Errorf("no data found in the specified range")
	}

	// Send the data to the node
	if args.NodeId != d.NodeId {
		reply.Timestamp = time.Now()
		shared.SendMessage(server, args.NodeId, reply)
	}

	return nil
}

// Worker: Function to receive data from other nodes
func (d *DataReplicationState) ReceiveData(server *rpc.Client, args DataReplicationResponse, leaderId int) error {
	// Check if the args is not outdated
	if time.Since(args.Timestamp).Seconds() > 2.0 {
		return nil
	}

	// Turn broadcast mode off if it was on
	// Used by the leader to prevent unnecessary broadcasts
	// If a node receives data, then it should not broadcast (meaning its a worker node)
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
		// Check for gaps between entries
		lastSeenId := d.DataItems[0].Id - 1
		
		for _, item := range d.DataItems {
			// Gap between the last seen ID and current item ID
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

	// Read data from file to check for missing ranges
	dataMap, diskErr := d.readDataMapFromFile()

	if diskErr == nil && len(dataMap) > 0 {
		// Find the highest ID in the file
		lastFileId := 0
		for id := range dataMap {
			if id > lastFileId {
				lastFileId = id
			}
		}
		
		fmt.Printf("Node %d: Last ID in file: %d\n", d.NodeId, lastFileId)
		
		// Check for missing IDs within the local file-stored data
		for i := 1; i <= lastFileId; i++ {
			if _, exists := dataMap[i]; !exists {
				// Find the range of consecutive missing IDs
				startMissing := i
				for j := i; j <= lastFileId; j++ {
					if _, exists := dataMap[j]; exists {
						missingRanges = append(missingRanges, [2]int{startMissing, j - 1})
						i = j
						break
					}
					if j == lastFileId {
						missingRanges = append(missingRanges, [2]int{startMissing, lastFileId})
						i = lastFileId
					}
				}
			}
		}
		
		// Check gap between file's last entry and in-memory's first entry
		if len(d.DataItems) > 0 {
			firstMemoryId := d.DataItems[0].Id
			
			// Add it to missing ranges
			if firstMemoryId > lastFileId + 1 {
				fmt.Printf("Node %d: Gap detected between file (last ID: %d) and memory (first ID: %d)\n", 
					d.NodeId, lastFileId, firstMemoryId)
				missingRanges = append(missingRanges, [2]int{lastFileId + 1, firstMemoryId - 1})
			}
		}	
	} else if !os.IsNotExist(diskErr) {
		fileName := fmt.Sprintf("%d-replication.json", d.NodeId)
		fmt.Printf("Node %d: Error reading data from file %s: %v\n", d.NodeId, fileName, diskErr)
		return diskErr
	}
	
	// Flag to indicate if we requested any data replication
	dataRequested := false

	// Handle missing ranges if any are found
	if len(missingRanges) > 0 {
		for _, r := range missingRanges {
			if diskErr == nil {
				// Simple check - if first and last elements exist to avoid unnecessary requests
				_, firstExists := dataMap[r[0]]
				_, lastExists := dataMap[r[1]]
				
				if firstExists && lastExists {
					fmt.Printf("Node %d: Elements of range [%d, %d] found in local file\n", 
						d.NodeId, r[0], r[1])
					continue
				}
			}
			
			fmt.Printf("Node %d: Missing data range from %d to %d\n", d.NodeId, r[0], r[1])
			dataRequested = true

			// Request data replication for the missing range
			args := DataReplicationRequest{
				Timestamp:   time.Now(),
				SenderId:    d.NodeId,
				StartDataId: r[0],
				EndDataId:   r[1],
			}
			shared.SendMessage(server, leaderId, args)
		}
	} 
	
	if !dataRequested && len(d.DataItems) >= 5 {
		if err := d.dumpDataToFile(); err != nil {
			if d.NodeId == leaderId {
				fmt.Printf("Node %d (Leader): Error dumping data: %v\n", d.NodeId, err)
			} else {
				fmt.Printf("Node %d: Error dumping data: %v\n", d.NodeId, err)
			}
		} else {
			// Clear in-memory data after successful dump
			d.DataItems = []DataItem{}
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
	fileName := fmt.Sprintf("%d-replication.json", d.NodeId)
	
	// Create a map with data items where the key is the ID
	dataMap := make(map[int]string)
	
	// First read existing data if file exists
	existingData, err := d.readDataMapFromFile()
	if err == nil {
		dataMap = existingData
	}
	
	// Add or update with new data items
	for _, item := range d.DataItems {
		dataMap[item.Id] = item.Value
	}
	
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %v", fileName, err)
	}
	defer file.Close()
	
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(dataMap); err != nil {
		return fmt.Errorf("failed to encode data to file %s: %v", fileName, err)
	}

	fmt.Printf("Node %d: Data dumped successfully to file %s (%d items)\n", 
		d.NodeId, fileName, len(dataMap))
	return nil
}

// Worker: Function to read data between a given range from the local disk file
func (d *DataReplicationState) ReadDataById(startId int, endId int) ([]DataItem, error) {
	// Read data map from file
	dataMap, err := d.readDataMapFromFile()
	if err != nil {
		return nil, fmt.Errorf("failed to read data map from file: %v", err)
	}
	
	// Extract data items within the specified range
	var data []DataItem
	for id, value := range dataMap {
		if id >= startId && id <= endId {
			data = append(data, DataItem{
				Id:    id,
				Value: value,
			})
		}
	}
	
	// Check if we found any data
	if len(data) == 0 {
		return nil, fmt.Errorf("no data found in the specified range: startId %d, endId %d", startId, endId)
	}
	
	return data, nil
}

// Leader: Function to read data to broadcast some data to replicate in this system
func ReadDataFile(startId int, endId int) (any, error) {
	file, err := os.Open("mr-out-final")
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()
	
	scanner := bufio.NewScanner(file)
	lineNum := 1
	var data []string
	
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

// Function to insert a data entry in sorted order based on Id
func (d *DataReplicationState) insertDataItemSorted(dataEntry DataItem) {
	insertIndex := 0
	for i, item := range d.DataItems {
		if item.Id == dataEntry.Id {
			d.DataItems[i] = dataEntry
			return
		}
		if item.Id > dataEntry.Id {
			insertIndex = i
			break
		}
		if i == len(d.DataItems) - 1 {
			insertIndex = len(d.DataItems)
		}
	}
	
	if len(d.DataItems) == 0 || insertIndex == len(d.DataItems) {
		d.DataItems = append(d.DataItems, dataEntry)
	} else {
		d.DataItems = append(d.DataItems[:insertIndex+1], d.DataItems[insertIndex:]...)
		d.DataItems[insertIndex] = dataEntry
	}
}

// Helper method to read data map from file
func (d *DataReplicationState) readDataMapFromFile() (map[int]string, error) {
	fileName := fmt.Sprintf("%d-replication.json", d.NodeId)
	
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return make(map[int]string), fmt.Errorf("file does not exist: %s", fileName)
	}
	
	file, err := os.Open(fileName)
	if err != nil {
		return make(map[int]string), fmt.Errorf("failed to open file %s: %v", fileName, err)
	}
	defer file.Close()
	
	var dataMap map[int]string
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&dataMap); err != nil {
		return make(map[int]string), fmt.Errorf("failed to decode data from file %s: %v", fileName, err)
	}
	
	return dataMap, nil
}

// Function to print DataItems array
func (d *DataReplicationState) PrintDataItems() {
	fmt.Printf("-------------------------------\n")
	for _, item := range d.DataItems {
		fmt.Printf("ID: %d, Value: %s\n", item.Id, item.Value)
	}
	fmt.Printf("-------------------------------\n")
}
