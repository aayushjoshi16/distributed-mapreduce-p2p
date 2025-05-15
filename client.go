package main

import (
	"encoding/gob"
	"fmt"
	// "lab4/mapreduce"
	"lab4/gossip"
	"lab4/raft"
	"lab4/shared"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	POLL_INTERVAL = 10
	Y_TIME        = 20
	Z_TIME_MAX    = 200
	Z_TIME_MIN    = 30
)

var FILES = [...]string{"data/pg-being_ernest.txt", "data/pg-metamorphosis.txt"}

func detectFailures(membership *gossip.Membership) {
	currTime := time.Now()

	// Get a copy of the members to avoid concurrent map access
	localCopy := make(map[int]gossip.Node)
	for id, val := range membership.Members {
		localCopy[id] = val
	}

	// Process the copy
	for _, val := range localCopy {
		if !val.Alive {
			continue
		}
		if currTime.After(val.Time.Add(gossip.FAIL_TIMEOUT * time.Second)) {
			// Mark as dead
			val.Alive = false
			// fmt.Printf("Node %d: Marked as Dead\n", val.ID)
		}
	}
}

type ClientState struct {
	id         int
	membership gossip.Membership
	raft       raft.RaftState
}

func NewState(id int, self_node gossip.Node, server *rpc.Client) ClientState {
	membership := gossip.NewMembership()
	membership.Add(self_node, nil)
	raft := raft.NewRaftState(server, id)

	return ClientState{
		id:         id,
		membership: membership,
		raft:       raft,
	}
}

func main() {
	gob.Register(gossip.Membership{})
	gob.Register(shared.GossipHeartbeat{})
	gob.Register(shared.RequestVote{})
	gob.Register(shared.RequestVoteResp{})
	gob.Register(shared.LeaderHeartbeat{})

	// Connect to RPC server
	server, _ := rpc.DialHTTP("tcp", "localhost:9005")

	args := os.Args[1:]

	// Get ID from command line argument
	if len(args) == 0 {
		fmt.Println("No args given")
		return
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("Found Error", err)
	}

	// Construct self
	self_node := gossip.Node{
		ID:        id,
		Hbcounter: 0,
		Time:      time.Now(),
		Alive:     true,
	}

	// Add node with input ID
	if err := server.Call("Membership.Add", self_node, nil); err != nil {
		fmt.Println("Error:2 Membership.Add()", err)
	} else {
		fmt.Printf("Success: Node created with id= %d\n", id)
	}

	state := NewState(id, self_node, server)

	time.AfterFunc(time.Millisecond*POLL_INTERVAL, func() { state.handlePoll(server) })

	var wg = sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

func (s *ClientState) handlePoll(server *rpc.Client) {
	var self_node gossip.Node
	s.membership.Get(s.id, &self_node)
	self_node.Hbcounter++
	self_node.Time = time.Now()
	self_node.Alive = true
	s.membership.Update(self_node, nil)

	s.raft.SendHeartbeats(server, s.id)

	detectFailures(&s.membership)

	for _, msg := range shared.ReadMessages(server, s.id) {
		switch smsg := msg.(type) {
		case shared.GossipHeartbeat:
			s.membership.MergeLeft(smsg.Membership)
			// printMembership(**membership)
		case shared.RequestVote:
			s.raft.RequestVote(server, smsg, s.id)
		case shared.RequestVoteResp:
			s.raft.VoteResponse(smsg, s.id)
		case shared.LeaderHeartbeat:
			// raft_timer.Reset(RAFT_X_TIME*time.Second + shared.RandomLeadTimeout())
			s.raft.HandleLeaderHeartbeat(server, smsg, s.id)
		}
	}

	time.AfterFunc(time.Millisecond*POLL_INTERVAL, func() { s.handlePoll(server) })
}

func printMembership(m gossip.Membership) {
	for i := range shared.MAX_NODES {
		var val, exists = m.Members[i+1]
		if exists {
			status := "is Alive"
			if !val.Alive {
				status = "is Dead"
			}
			fmt.Printf("Node %d has hb %d, time %s and %s\n", val.ID, val.Hbcounter, val.Time.Format("03:04:05"), status)
		}
	}
	fmt.Println("")
}
