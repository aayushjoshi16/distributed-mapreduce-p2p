package shared

import (
	"fmt"
	"lab4/gossip"
	"net/rpc"

	// "fmt"
	"math/rand"
	"sync"
	"time"
)

const MAX_NODES = 8

type GossipHeartbeat struct {
	Membership gossip.Membership
}

type RequestVote struct {
	Term        int
	CandidateId int
}

type RequestVoteResp struct {
	Term int
	Vote bool
}

type LeaderHeartbeat struct {
	Term     int
	LeaderId int
}

// Helper func to create random time not heard from leader to become candidate
func RandomLeadTimeout() time.Duration {
	var d = rand.Intn(150) + 150
	return time.Duration(d) * time.Millisecond
}

// Request struct represents a new message request to a client
type Request struct {
	ID   int
	Data any
}

// Requests struct represents pending message requests
type Requests struct {
	Pending map[int][]any
	Mutex   sync.RWMutex
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

// Send the current membership table to a neighboring node with the provided ID
func SendMessage(server *rpc.Client, id int, data any) {
	req := Request{ID: id, Data: data}
	var reply bool
	err := server.Call("Requests.Add", req, &reply)
	if err != nil {
		fmt.Println("Error: Requests.Add", err)
	} else {
		//fmt.Printf("Success: Sent membership to node %d\n", id)
	}
}

// Read incoming messages from other nodes
func ReadMessages(server *rpc.Client, id int) []interface{} {
	var reply []any
	err := server.Call("Requests.Listen", id, &reply)
	if err != nil {
		fmt.Println("Error: Requests.Listen()", err)
	} else {
		//fmt.Printf("Success: Received membership from node %d\n", id)

	}
	return reply
}
