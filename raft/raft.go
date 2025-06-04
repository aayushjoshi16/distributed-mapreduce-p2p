package raft

import (
	"fmt"
	"lab4/shared"
	"lab4/replication"
	"math/rand"
	"net/rpc"
	"time"
	"sync"
)

type Role int

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader

	RAFT_X_TIME = 1500
	RAFT_Y_MIN  = 1000
	RAFT_Y_MAX  = 1500
	RAFT_Z_TIME = 250
)

// type RequestVote struct {
// 	Timestamp   time.Time
// 	Term        int
// 	CandidateId int
// }

// type RequestVoteResp struct {
// 	Timestamp   time.Time
// 	Term int
// 	Vote bool
// }

// type LeaderHeartbeat struct {
// 	Timestamp   time.Time
// 	Term     int
// 	LeaderId int
// }

type RaftState struct {
	Role           Role
	election_timer *time.Timer
	election_mutex sync.RWMutex
	election_limit int
	Term           int
	cur_vote       *int
	num_votes      int
	leader         *int
}

func NewRaftState(server *rpc.Client, id int) *RaftState {
	s := RaftState{
		Role:           RoleFollower,
		election_timer: nil,
		election_limit: 5,
		Term:           0,
		cur_vote:       nil,
		num_votes:      0,
		leader:         nil,
	}
	s.ResetElectionTimer(server, id)
	return &s
}

// PauseElections prevents this node from starting elections by acquiring a write lock
func (s *RaftState) PauseElections() {
    s.election_mutex.Lock()
	s.election_limit = 5
    fmt.Printf("Election timeout blocked\n")
}

// ResumeElections allows this node to participate in elections again
func (s *RaftState) ResumeElections() {
    s.election_mutex.Unlock()
	s.election_limit = 0
    fmt.Printf("Election timeout resumed\n")
}

// RequestVotes for candidate calling on them
func (s *RaftState) ShouldRequestVote(server *rpc.Client, args shared.RequestVote, id int) bool {
	fmt.Printf("Node %d: Received vote request from %d with Term %d\n", id, args.CandidateId, args.Term)
	
	// Check if the request is stale
	if time.Since(args.Timestamp).Seconds() > 2.0 {
		return false
	}
	
	// Check if the Term is valid
	if args.Term < s.Term {
		return false
	}

	// If new Term, update current Term and revert to follower
	if args.Term > s.Term {
		s.Term = args.Term
		s.Role = RoleFollower
		s.cur_vote = nil
	}

	// Skip vote if a candidate or leader
	if s.Role == RoleLeader || s.Role == RoleCandidate {
		return false
	}

	// If already voted this Term
	if s.cur_vote != nil && *s.cur_vote != args.CandidateId {
		return false
	}

	// Grant vote if haven't voted in this Term or already voted for this candidate
	fmt.Printf("Node %d: Granted vote to %d for Term %d\n", id, args.CandidateId, s.Term)
	return true
}

func (s *RaftState) RequestVote(server *rpc.Client, args shared.RequestVote, id int) {
	if s.ShouldRequestVote(server, args, id) {
		s.cur_vote = &args.CandidateId
		s.ResetElectionTimer(server, id)
		shared.SendMessage(server, args.CandidateId, shared.RequestVoteResp{
			Term: s.Term,
			Vote: true,
			Timestamp: time.Now(),
		})
	} else {
		shared.SendMessage(server, args.CandidateId, shared.RequestVoteResp{
			Term: s.Term,
			Vote: false,
			Timestamp: time.Now(),
		})
	}
}

// Reset the election timeout timer with random duration
func (s *RaftState) ResetElectionTimer(server *rpc.Client, id int) {
	if s.election_timer != nil {
		s.election_timer.Stop()
		s.election_timer = nil
	}

	// Create a random election timeout (between 150-300ms)
	electionTimeout := time.Duration(RAFT_X_TIME+rand.Intn(RAFT_Y_MAX)+RAFT_Y_MIN) * time.Millisecond

	// Reset voted for and all other variables to default to start a new election
	// votes = 0
	// votedFor = nil
	// role = raft.RoleFollower
	// leaderID = nil

	// Reset or create the timer
	s.election_timer = time.AfterFunc(electionTimeout, func() {
		// Try to acquire read lock - will block if PauseElections is holding write lock
		if !s.election_mutex.TryRLock() {
			s.election_limit--
			fmt.Printf("Node %d: Election timeout skipped (limit: %d)\n", id, s.election_limit)

			// If election limit is reached, release the lock to allow elections
			if s.election_limit <= 0 {
				s.election_mutex.Unlock() // Unlock directly instead of calling ResumeElections
				fmt.Printf("Node %d: Election timeout limit reached, starting election\n", id)
				// s.StartElection(server, id) // Start an election immediately
				// return
			}

			s.ResetElectionTimer(server, id)
			return
		}
		// We got the read lock, so elections are allowed
		s.election_mutex.RUnlock()
		
		fmt.Printf("Node %d: Election timeout\n", id)
		s.StartElection(server, id)
	})
}

// Start a new election
func (s *RaftState) StartElection(server *rpc.Client, id int) {
	// Only start election if still a follower (or candidate with expired election)
	if s.Role == RoleLeader {
		return
	}

	// Increment Term and vote for self
	s.Term++
	s.cur_vote = &id
	s.Role = RoleCandidate
	s.num_votes = 1 // Vote for self

	fmt.Printf("Node %d: Starting election for Term %d\n", id, s.Term)

	// Request votes from all nodes
	for i := 1; i <= shared.MAX_NODES; i++ {
		if i == id {
			continue // Skip self
		}

		// Send RequestVote to each node
		voteReq := shared.RequestVote{
			Term:        s.Term,
			CandidateId: id,
			Timestamp:   time.Now(),
		}
		shared.AsyncSendMessage(server, i, voteReq)
	}

	// Reset election timeout in case we don't get majority
	s.ResetElectionTimer(server, id)
}

// Handle vote responses
func (s *RaftState) VoteResponse(resp shared.RequestVoteResp, id int) {
	// Check if request is stale
	if time.Since(resp.Timestamp).Seconds() > 2.0 {
		return
	}

	// If received a higher Term, revert to follower
	if resp.Term > s.Term {
		s.Term = resp.Term
		s.Role = RoleFollower
		s.cur_vote = nil
		s.num_votes = 0 // Reset votes
		return
	}

	// Only count votes if still a candidate
	if s.Role == RoleCandidate && s.Term == resp.Term && resp.Vote {
		s.num_votes++

		fmt.Printf("Node %d: Total votes: %d\n", id, s.num_votes)

		// If we have majority, become leader
		if s.num_votes > shared.MAX_NODES/2 {
			// if votes == 2 {
			if s.Role != RoleCandidate {
				return
			}

			s.Role = RoleLeader
			fmt.Printf("Node %d: Became leader for Term %d\n", id, s.Term)

			// Stop election timer as leaders don't timeout
			if s.election_timer != nil {
				s.election_timer.Stop()
			}
		}
	}
}

// Send leader heartbeat to all nodes
func (s *RaftState) SendHeartbeats(server *rpc.Client, id int) {
	if s.Role != RoleLeader {
		return
	}

	for i := 1; i <= shared.MAX_NODES; i++ {
		if i == id {
			continue // Skip self
		}

		heartbeat := shared.LeaderHeartbeat{
			Term:     s.Term,
			LeaderId: id,
			Timestamp: time.Now(),
		}
		shared.SendMessage(server, i, heartbeat)
	}
}

// Handle leader heartbeats
func (s *RaftState) HandleLeaderHeartbeat(server *rpc.Client, heartbeat shared.LeaderHeartbeat, id int) {
	// Check if request is stale
	if time.Since(heartbeat.Timestamp).Seconds() > 2.0 {
		return
	}

	// If Term is outdated, ignore
	if heartbeat.Term < s.Term {
		return
	}

	// If new Term or valid heartbeat from current Term
	if heartbeat.Term >= s.Term {
		// Update Term if needed
		if heartbeat.Term > s.Term {
			// Send message to the new leader for pending data replication
			fmt.Printf("Node %d: Sending data replication request to leader %d for Term %d\n", id, heartbeat.LeaderId, heartbeat.Term)
			dataReplication := replication.DataReplicationRequest{
				Timestamp: 	time.Now(),
				SenderId:   id,
				Term: 		s.Term,
				LastDataId: 0,	 		// 0 meaning no data is with this node
			}
			shared.SendMessage(server, heartbeat.LeaderId, dataReplication)

			// Update state
			s.Term = heartbeat.Term
			s.cur_vote = nil
		}

		// Reset to follower (even if already follower)
		s.Role = RoleFollower

		// Assign as current leader
		s.leader = &heartbeat.LeaderId

		// Reset election timer
		s.ResetElectionTimer(server, id)
	}
}

func (s *RaftState) GetLeader() *int {
	// I LOVE GO
	if s.leader == nil {
		return nil
	}
	l := new(int)
	*l = *s.leader
	return l
}
