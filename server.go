package main

import (
	"lab4/shared"
	// "lab4/mapreduce"
	"fmt"
	"encoding/gob"
	"net/http"
	"net/rpc"
)

func main() {
	// create a Membership list
	nodes := shared.NewMembership()
	requests := shared.NewRequests()

	// register nodes with `rpc.DefaultServer`
	rpc.Register(nodes)
	rpc.Register(requests)
	gob.Register(shared.Membership{})
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
