package main

import (
	"lab4/gossip"
	"lab4/shared"
	// "lab4/mapreduce"
	"encoding/gob"
	"fmt"
	"net/http"
	"net/rpc"
)

func main() {
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

	// register an HTTP handler for RPC communication
	rpc.HandleHTTP()

	fmt.Println("Server started on localhost:9005")

	// listen and serve default HTTP server
	http.ListenAndServe("localhost:9005", nil)
}
