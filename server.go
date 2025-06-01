package main

import (
	"lab4/gossip"
	"lab4/mapreduce"
	"lab4/shared"
	"os"

	// "lab4/mapreduce"
	"encoding/gob"
	"fmt"
	"net/http"
	"net/rpc"
)

func main() {
	if _, err := os.Stat("./data"); err != nil {
		fmt.Println("Data directory not found. Please create a data directory with the required files.")
		return
	} else {
		fmt.Println("Data directory found.")
	}

	// create a Membership list
	nodes := gossip.NewMembership()
	requests := shared.NewRequests()

	// register nodes with `rpc.DefaultServer`

	rpc.Register(&nodes)
	rpc.Register(requests)

	gob.Register(gossip.Membership{})
	gob.Register(shared.GossipHeartbeat{})
	gob.Register(shared.RequestVote{})
	gob.Register(shared.RequestVoteResp{})
	gob.Register(shared.LeaderHeartbeat{})

	gob.Register(mapreduce.DataReplication{})
	gob.Register(mapreduce.GetTaskArgs{})
	gob.Register(mapreduce.GetTaskReply{})
	gob.Register(mapreduce.ReportTaskArgs{})
	gob.Register(mapreduce.KeyValue{})
	gob.Register(mapreduce.MasterTask{})

	// go detectAndReset(&nodes, tasks)

	// register an HTTP handler for RPC communication
	rpc.HandleHTTP()

	fmt.Println("Server started on localhost:9005")

	// listen and serve default HTTP server
	http.ListenAndServe("localhost:9005", nil)
}
