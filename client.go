package main

import (
	"encoding/gob"
	"fmt"

	// "lab4/mapreduce"
	"lab4/gossip"
	"lab4/mapreduce"
	"lab4/raft"
	"lab4/shared"
	"lab4/wc"
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

var FILES = [...]string{
	"data/19626.txt",
	"data/pg-being_ernest.txt",
	"data/pg-metamorphosis.txt",
	"data/pg84.txt",
	"data/pg1342.txt",
	"data/pg1513.txt",
	"data/pg2701.txt",
	"data/pg16389.txt",
}

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
	raft       *raft.RaftState
	isActive   bool
	isDone     bool
	taskChan   chan mapreduce.GetTaskReply
	tracker    *mapreduce.MasterTask
}

func NewState(id int, self_node gossip.Node, server *rpc.Client) ClientState {
	membership := gossip.NewMembership()
	membership.Add(self_node, nil)
	raft := raft.NewRaftState(server, id)

	return ClientState{
		id:         id,
		membership: membership,
		raft:       raft,
		isActive:   false,
		isDone:     false,
		taskChan:   make(chan mapreduce.GetTaskReply, 1),
		tracker:    nil,
	}
}

func main() {
	gob.Register(gossip.Membership{})
	gob.Register(shared.GossipHeartbeat{})
	gob.Register(shared.RequestVote{})
	gob.Register(shared.RequestVoteResp{})
	gob.Register(shared.LeaderHeartbeat{})

	gob.Register(mapreduce.GetTaskArgs{})
	gob.Register(mapreduce.GetTaskReply{})
	gob.Register(mapreduce.ReportTaskArgs{})
	gob.Register(mapreduce.KeyValue{})
	gob.Register(mapreduce.MasterTask{})

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

	time.AfterFunc(time.Millisecond*POLL_INTERVAL, func() {
		fmt.Printf("Node %d: Polling...\n", id)
		state.handlePoll(server)
	})

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
			// we have a leader GO GO GO
			if !s.isActive && !s.isDone {
				// technically there is a race condition here. I really hope it doesn't matter.
				s.isActive = true
				go s.runMapReduceWorker(server)
			}
		case mapreduce.GetTaskArgs:
			// should only recieve this if leader
			if s.raft.Role == raft.RoleLeader {
				if s.tracker == nil {
					s.tracker = mapreduce.MakeMaster(FILES[:], 8)
				}
				reply := mapreduce.GetTaskReply{}
				if err := s.tracker.GetTask(smsg, &reply); err != nil {
					reply.NoTasks = true
				} else {
					reply.NoTasks = false
				}
				shared.SendMessage(server, smsg.SenderId, reply)
			}
		case mapreduce.ReportTaskArgs:
			if s.raft.Role == raft.RoleLeader {
				if s.tracker == nil {
					s.tracker = mapreduce.MakeMaster(FILES[:], 8)
				}
				s.tracker.ReportTaskDone(smsg)
			}
		case mapreduce.GetTaskReply:
			s.taskChan <- smsg
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

func (s *ClientState) reportTaskComplete(server *rpc.Client, taskType mapreduce.TaskPhase, taskID int) {
	if server == nil {
		fmt.Printf("Node %d: Error - RPC client is nil\n", s.id)
		return
	}

	masterId := s.raft.GetLeader()

	args := mapreduce.ReportTaskArgs{
		TaskType: taskType,
		TaskID:   taskID,
	}
	// error handling smirror handling
	// reply := mapreduce.ReportTaskReply{}

	shared.SendMessage(server, *masterId, args)

	fmt.Printf("Node %d: Successfully reported completion of task %d\n", s.id, taskID)

}

func (s *ClientState) runMapReduceWorker(server *rpc.Client) {
	fmt.Printf("Node %d: Starting MapReduce worker\n", s.id)

	// Skip election timeouts
	s.raft.PauseElections()
    defer s.raft.ResumeElections()

	masterId := s.raft.GetLeader()
	if masterId == nil {
		s.isActive = false
		return
	}
	fmt.Printf("Node %d: Worker found leader %d\n", s.id, *masterId)

	for !s.isDone {
		// TEMPORARY
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("Iteration...\n")

		args := mapreduce.GetTaskArgs{
			SenderId: s.id,
		}

		// Using Call instead of Go to avoid the nil pointer issue
		shared.SendMessage(server, *masterId, args)
		reply := <-s.taskChan

		if !reply.NoTasks {
			switch reply.TaskType {
			case mapreduce.MapPhase:
				fmt.Printf("Node %d: Running Map Task %d on file %s\n", s.id, reply.MapTaskID, reply.FileName)

				// Verify file exists
				if _, err := os.Stat(reply.FileName); os.IsNotExist(err) {
					fmt.Printf("Node %d: File does not exist: %s\n", s.id, reply.FileName)
					time.Sleep(1 * time.Second)
					continue
				}

				err := mapreduce.ExecuteMTask(reply.FileName, reply.MapTaskID, reply.NReduce, wc.Map)
				if err != nil {
					fmt.Printf("Node %d: Error executing map task %d: %v\n", s.id, reply.MapTaskID, err)
					time.Sleep(1 * time.Second)
					continue
				}

				fmt.Printf("Node %d: Completed Map Task %d\n", s.id, reply.MapTaskID)
				s.reportTaskComplete(server, mapreduce.MapPhase, reply.MapTaskID)

			case mapreduce.ReducePhase:
				fmt.Printf("Node %d: Running Reduce Task %d\n", s.id, reply.ReduceTaskID)

				// Use the correct NMap value
				nReduce := reply.NReduce
				if nReduce == 0 {
					fmt.Printf("Node %d: Warning - NMap is 0\n", s.id)
					continue
				}

				err := mapreduce.ExecuteRTask(reply.ReduceTaskID, nReduce, wc.Reduce)
				if err != nil {
					fmt.Printf("Node %d: Error executing reduce task %d: %v\n", s.id, reply.ReduceTaskID, err)
					time.Sleep(1 * time.Second)
					continue
				}

				fmt.Printf("Node %d: Completed Reduce Task %d\n", s.id, reply.ReduceTaskID)
				s.reportTaskComplete(server, mapreduce.ReducePhase, reply.ReduceTaskID)

			case mapreduce.DonePhase:
				fmt.Printf("Node %d: All tasks are done\n", s.id)
				s.isDone = true
				s.isActive = false
				fmt.Printf("Node %d: Exiting MapReduce worker\n", s.id)
				return

			default:
				fmt.Printf("Node %d: Unknown task type %d\n", s.id, reply.TaskType)
				// time.Sleep(1 * time.Second)
			}
		}

		// Small delay between task requests
		time.Sleep(100 * time.Millisecond)
	}
}
