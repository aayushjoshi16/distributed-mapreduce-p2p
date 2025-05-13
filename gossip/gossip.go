package gossip

import (
	"errors"
	"math/rand"
	"time"
)

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

// Membership struct represents participanting nodes
type Membership struct {
	Members map[int]Node
}

// Returns a new instance of a Membership (pointer).
func NewMembership() Membership {
	return Membership{
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

func (table1 *Membership) MergeLeft(table2 Membership) {
	currTime := time.Now()

	for id, node := range table1.Members {
		if node.Alive && currTime.After(node.Time.Add(FAIL_TIMEOUT*time.Second)) {
			node.Alive = false
			table1.Members[id] = node
		}
	}

	for id, node2 := range table2.Members {
		if node1, exists := table1.Members[id]; exists {
			if node2.Hbcounter > node1.Hbcounter && node2.Alive {
				table1.Members[id] = node2
			} else if node2.Hbcounter < node1.Hbcounter && !node1.Alive && node2.Alive { // allow revival based on restart
				table1.Members[id] = node2
			}
		} else {
			table1.Members[id] = node2
		}
	}
}
