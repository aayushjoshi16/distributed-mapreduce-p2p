package main

import (
	"encoding/gob"
	"fmt"
	// "lab4/mapreduce"
	"lab4/raft"
	"lab4/shared"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	MAX_NODES  = 8
	X_TIME     = 10
	Y_TIME     = 20
	Z_TIME_MAX = 200
	Z_TIME_MIN = 30
)

var FILES = [...]string{"data/pg-being_ernest.txt", "data/pg-metamorphosis.txt"}

var wg = &sync.WaitGroup{}
var membership_lock = sync.RWMutex{}
var raft_timer *time.Timer
var role raft.Role = raft.RoleFollower
var currentTerm int = 0
var votedFor *int = nil
var leaderID *int = nil
var self_node shared.Node
var votes int = 0
var electionTimeout time.Duration

// Send the current membership table to a neighboring node with the provided ID
func sendMessage(server *rpc.Client, id int, data any) {
	//TODO

	req := shared.Request{ID: id, Data: data}
	var reply bool
	err := server.Call("Requests.Add", req, &reply)
	if err != nil {
		fmt.Println("Error: Requests.Add", err)
	} else {
		//fmt.Printf("Success: Sent membership to node %d\n", id)
	}
}

func calcTime() time.Time {
	return time.Now()
}

// Read incoming messages from other nodes
func readMessages(server *rpc.Client, id int) []interface{} {
	//TODO
	var reply []any
	err := server.Call("Requests.Listen", id, &reply)
	if err != nil {
		fmt.Println("Error: Requests.Listen()", err)
	} else {
		//fmt.Printf("Success: Received membership from node %d\n", id)

	}
	return reply
}

func detectFailures(membership *shared.Membership) {
	currTime := calcTime()

	// Get a copy of the members to avoid concurrent map access
	localCopy := make(map[int]shared.Node)
	for id, val := range membership.Members {
		localCopy[id] = val
	}

	// Process the copy
	for _, val := range localCopy {
		if !val.Alive {
			continue
		}
		if currTime.After(val.Time.Add(shared.FAIL_TIMEOUT * time.Second)) {
			// Mark as dead
			val.Alive = false
			// fmt.Printf("Node %d: Marked as Dead\n", val.ID)
		}
	}
}

// RequestVotes for candidate calling on them
func requestVote(server *rpc.Client, args shared.RequestVote) bool {
	fmt.Printf("Node %d: Received vote request from %d with term %d\n", self_node.ID, args.CandidateId, args.Term)
	// fmt.Printf("Recieved vote request from %d with term %d, current %d, votedFor %s\n", args.CandidateId, args.Term, currentTerm, votedFor)
	// Check if the term is valid
	if args.Term < currentTerm {
		return false
	}

	// If new term, update current term and revert to follower
	if args.Term > currentTerm {
		currentTerm = args.Term
		role = raft.RoleFollower
		votedFor = nil
	}

	// Skip vote if a candidate or leader
	if role == raft.RoleLeader || role == raft.RoleCandidate {
		return false
	}

	// If already voted this term
	if votedFor != nil && *votedFor != args.CandidateId {
		return false
	}

	// Grant vote if haven't voted in this term or already voted for this candidate
	intVar := args.CandidateId
	votedFor = &intVar
	// voteGranted = true
	fmt.Printf("Node %d: Granted vote to %d for term %d\n", self_node.ID, args.CandidateId, currentTerm)
	// fmt.Printf("Granted vote request from %d with term %d, current %d, votedFor %s\n", args.CandidateId, args.Term, currentTerm, *votedFor)

	resetElectionTimer(server)
	// }

	return true
}

// Handle vote responses
func voteResponse(resp shared.RequestVoteResp) {
	// If received a higher term, revert to follower
	if resp.Term > currentTerm {
		currentTerm = resp.Term
		role = raft.RoleFollower
		votedFor = nil
		votes = 0 // Reset votes
		return
	}

	// fmt.Printf("Node %d: Received vote response for term %d role %s and vote %s\n", self_node.ID, resp.Term, role, resp.Vote)

	// Only count votes if still a candidate
	if role == raft.RoleCandidate && currentTerm == resp.Term && resp.Vote {
		votes++

		fmt.Printf("Node %d: Total votes: %d\n", self_node.ID, votes)

		// If we have majority, become leader
		if votes > MAX_NODES/2 {
			// if votes == 2 {
			if role != raft.RoleCandidate {
				return
			}

			role = raft.RoleLeader
			fmt.Printf("Node %d: Became leader for term %d\n", self_node.ID, currentTerm)

			// Stop election timer as leaders don't timeout
			if raft_timer != nil {
				raft_timer.Stop()
			}
		}
	}
}

// Reset the election timeout timer with random duration
func resetElectionTimer(server *rpc.Client) {
	// Create a random election timeout (between 150-300ms)
	// electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	electionTimeout = time.Duration(raft.RAFT_X_TIME+rand.Intn(raft.RAFT_Y_MAX)+raft.RAFT_Y_MIN) * time.Millisecond

	// Reset voted for and all other variables to default to start a new election
	// votes = 0
	// votedFor = nil
	// role = raft.RoleFollower
	// leaderID = nil

	// Reset or create the timer
	if raft_timer != nil {
		raft_timer.Stop()
	}

	raft_timer = time.AfterFunc(electionTimeout, func() {
		startElection(server)
	})
}

// Start a new election
func startElection(server *rpc.Client) {
	membership_lock.Lock()
	defer membership_lock.Unlock()

	// Only start election if still a follower (or candidate with expired election)
	if role == raft.RoleLeader {
		return
	}

	// Increment term and vote for self
	currentTerm++
	myID := self_node.ID
	votedFor = &myID
	role = raft.RoleCandidate
	votes = 1 // Vote for self

	fmt.Printf("Node %d: Starting election for term %d\n", myID, currentTerm)

	// Request votes from all nodes
	for i := 1; i <= MAX_NODES; i++ {
		if i == myID {
			continue // Skip self
		}

		// Send RequestVote to each node
		voteReq := shared.RequestVote{
			Term:        currentTerm,
			CandidateId: myID,
		}
		sendMessage(server, i, voteReq)
	}

	// Reset election timeout in case we don't get majority
	resetElectionTimer(server)
}

// Send leader heartbeat to all nodes
func sendHeartbeats(server *rpc.Client) {
	if role != raft.RoleLeader {
		return
	}

	for i := 1; i <= MAX_NODES; i++ {
		if i == self_node.ID {
			continue // Skip self
		}

		heartbeat := shared.LeaderHeartbeat{
			Term:     currentTerm,
			LeaderId: self_node.ID,
		}
		sendMessage(server, i, heartbeat)
	}
}

// Handle leader heartbeats
func handleLeaderHeartbeat(server *rpc.Client, heartbeat shared.LeaderHeartbeat) {
	// If term is outdated, ignore
	if heartbeat.Term < currentTerm {
		return
	}

	// If new term or valid heartbeat from current term
	if heartbeat.Term >= currentTerm {
		// Update term if needed
		if heartbeat.Term > currentTerm {
			currentTerm = heartbeat.Term
			votedFor = nil
		}

		// Reset to follower (even if already follower)
		role = raft.RoleFollower

		// Reset election timer
		resetElectionTimer(server)

		fmt.Printf("Node %d: Received heartbeat from leader %d (term %d)\n",
			self_node.ID, heartbeat.LeaderId, heartbeat.Term)
	}
}

func main() {
	gob.Register(shared.Membership{})
	gob.Register(shared.GossipHeartbeat{})
	gob.Register(shared.RequestVote{})
	gob.Register(shared.RequestVoteResp{})
	gob.Register(shared.LeaderHeartbeat{})

	Z_TIME := (rand.Intn(Z_TIME_MAX-Z_TIME_MIN) + Z_TIME_MIN)

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

	fmt.Println("Node", id, "will fail after", Z_TIME, "seconds")

	currTime := calcTime()
	// Construct self
	self_node = shared.Node{
		ID:        id,
		Hbcounter: 0,
		Time:      currTime,
		Alive:     true,
	}

	// Add node with input ID
	if err := server.Call("Membership.Add", self_node, nil); err != nil {
		fmt.Println("Error:2 Membership.Add()", err)
	} else {
		fmt.Printf("Success: Node created with id= %d\n", id)
	}

	neighbors := self_node.InitializeNeighbors(id)
	fmt.Println("Neighbors:", neighbors)

	membership := shared.NewMembership()
	membership.Update(self_node, nil)

	sendMessage(server, neighbors[0], *membership)

	role = raft.RoleFollower
	resetElectionTimer(server)

	// crashTime := self_node.CrashTime()
	// raft_timer = time.AfterFunc(RAFT_X_TIME*time.Second + shared.RandomLeadTimeout(), func() { startLeaderCheck(server, &self_node, id) })

	time.AfterFunc(time.Millisecond*X_TIME, func() { runAfterX(server, &self_node, &membership, id) })
	time.AfterFunc(time.Millisecond*Y_TIME, func() { runAfterY(server, neighbors, &membership, id) })
	// time.AfterFunc(time.Second*time.Duration(Z_TIME), func() { runAfterZ(server, id) })

	wg.Add(1)
	wg.Wait()
}

func runAfterX(server *rpc.Client, node *shared.Node, membership **shared.Membership, id int) {
	//TODO
	self_node.Hbcounter++
	self_node.Time = calcTime()
	self_node.Alive = true

	membership_lock.Lock()
	(*membership).Update(self_node, nil)
	membership_lock.Unlock()

	if role == raft.RoleLeader {
		sendHeartbeats(server)
	}

	time.AfterFunc(time.Millisecond*X_TIME, func() { runAfterX(server, node, membership, id) })
}

func runAfterY(server *rpc.Client, neighbors [2]int, membership **shared.Membership, id int) {
	membership_lock.Lock()
	detectFailures(*membership)
	membership_lock.Unlock()

	for _, msg := range readMessages(server, id) {
		switch smsg := msg.(type) {
		case shared.GossipHeartbeat:
			membership_lock.Lock()
			*membership = shared.CombineTables(*membership, &smsg.Membership)
			membership_lock.Unlock()
			// printMembership(**membership)
		case shared.RequestVote:
			sendMessage(server, smsg.CandidateId, shared.RequestVoteResp{Term: currentTerm, Vote: requestVote(server, smsg)})
		case shared.RequestVoteResp:
			voteResponse(smsg)
		case shared.LeaderHeartbeat:
			// raft_timer.Reset(RAFT_X_TIME*time.Second + shared.RandomLeadTimeout())
			handleLeaderHeartbeat(server, smsg)
		}
	}
	neighbors = self_node.InitializeNeighbors(id)
	time.AfterFunc(time.Millisecond*Y_TIME, func() { runAfterY(server, neighbors, membership, id) })
}

func runAfterZ(server *rpc.Client, id int) {
	//TODO
	self_node.Alive = false
	fmt.Printf("Node %d: Crashed\n", id)
	fmt.Printf("Heartbeat Counter: %d\n", self_node.Hbcounter)
	fmt.Printf("Time: %v\n", self_node.Time)

	// FIX: Use a Node pointer for the reply instead of a bool
	var reply shared.Node
	err := server.Call("Membership.Update", self_node, &reply)
	if err != nil {
		fmt.Println("Error: Membership.Update()", err)
	} else {
		fmt.Printf("Success: Node %d marked as dead\n", id)
	}

	wg.Done()
}

func printMembership(m shared.Membership) {
	for i := range MAX_NODES {
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
