package shared

import (
	"errors"
	// "fmt"
	"math/rand"
	"sync"
	"time"
)

type GossipHeartbeat struct {
	Membership Membership
}

type RequestVote struct {
	Term        int
	CandidateId int
}

type RequestVoteResp struct {
	Term		int
	Vote 		bool
}

type LeaderHeartbeat struct{
	Term 		int
	LeaderId 	int
}

const (
	MAX_NODES    = 8
	FAIL_TIMEOUT = 1.0
)

// Node struct represents a computing node.
type Node struct {
	ID        int
	Hbcounter int
	Time      time.Time
	Alive     bool
}

// Generate random crash time from 10-60 seconds
func (n Node) CrashTime() int {
	max := 200
	min := 10
	return rand.Intn(max-min) + min
}

func (n Node) InitializeNeighbors(id int) [2]int {
	neighbor1 := RandInt()
	for neighbor1 == id {
		neighbor1 = RandInt()
	}
	neighbor2 := RandInt()
	for neighbor1 == neighbor2 || neighbor2 == id {
		neighbor2 = RandInt()
	}
	return [2]int{neighbor1, neighbor2}
}

func RandInt() int {
	return rand.Intn(MAX_NODES-1+1) + 1
}

// Helper func to create random time not heard from leader to become candidate
func RandomLeadTimeout() time.Duration {
	var d = rand.Intn(150) + 150
	return time.Duration(d) * time.Millisecond
}

/*---------------*/

// Membership struct represents participanting nodes
type Membership struct {
	Members map[int]Node
}

// Returns a new instance of a Membership (pointer).
func NewMembership() *Membership {
	return &Membership{
		Members: make(map[int]Node),
	}
}

// Adds a node to the membership list.
func (m *Membership) Add(payload Node, reply *Node) error {
	m.Members[payload.ID] = payload
	return nil
}

// Updates a node in the membership list.
func (m *Membership) Update(payload Node, reply *Node) error {
	if exisiting, ok := m.Members[payload.ID]; ok {
		if payload.Hbcounter > exisiting.Hbcounter || (payload.Alive && !exisiting.Alive) {
			m.Members[payload.ID] = payload
		}
	} else {
		m.Members[payload.ID] = payload
	}
	return nil
}

// Returns a node with specific ID.
func (m *Membership) Get(payload int, reply *Node) error {
	// Check if payload.ID exists in the membership list
	nod, ok := m.Members[payload]
	if ok {
		*reply = nod
		return nil
	} else {
		return errors.New("Node does not exist")
	}
}

/*---------------*/

// Request struct represents a new message request to a client
type Request struct {
	ID   int
	Data any
}

// Requests struct represents pending message requests
type Requests struct {
	Pending map[int][]any
	Mutex sync.RWMutex
}

// Returns a new instance of a Membership (pointer).
func NewRequests() *Requests {
	return &Requests{
		Pending: make(map[int][]any),
	}
}

// Adds a new message request to the pending list
func (req *Requests) Add(payload Request, reply *bool) error {
	//TODO
	// fmt.Printf("Queuing message for %d with %s\n", payload.ID, payload.Data)
	req.Mutex.Lock()
	defer req.Mutex.Unlock()
	pend, exists := req.Pending[payload.ID]
	if exists {
		req.Pending[payload.ID] = append(pend, payload.Data)
	} else {
		req.Pending[payload.ID] = make([]any, 1)
		req.Pending[payload.ID][0] = payload.Data
	}
	*reply = true
	return nil
}

// Listens to communication from neighboring nodes.
func (req *Requests) Listen(ID int, reply *[]any) error {
	// fmt.Printf("ID %d listening to messages\n", ID)
	req.Mutex.Lock()
	defer req.Mutex.Unlock()
	if membership, exists := req.Pending[ID]; exists {
		*reply = membership
		delete(req.Pending, ID) // Remove the request after processing
		return nil
	}
	*reply = make([]any, 0) // Return an empty membership if not found
	return nil
}

func CombineTables(table1 *Membership, table2 *Membership) *Membership {
	result := NewMembership()
	currTime := time.Now()

	for id, node := range table1.Members {
		result.Members[id] = node
		if node.Alive && currTime.After(node.Time.Add(FAIL_TIMEOUT * time.Second)) {
			node.Alive = false
			result.Members[id] = node
		}
	}

	for id, node2 := range table2.Members {
		if node1, exists := result.Members[id]; exists {
			if node2.Hbcounter > node1.Hbcounter && node2.Alive {
				result.Members[id] = node2
			} else if node2.Hbcounter < node1.Hbcounter && !node1.Alive && node2.Alive { // allow revival based on restart
				result.Members[id] = node2
			}
		} else {
			result.Members[id] = node2
		}
	}
	return result
}
