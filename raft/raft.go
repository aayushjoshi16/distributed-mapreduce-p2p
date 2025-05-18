package raft

import (
	"fmt"
	"lab4/shared"
	"math/rand"
	"net/rpc"
	"time"
)

type Role int

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader

	RAFT_X_TIME = 1000
	RAFT_Y_MIN  = 150
	RAFT_Y_MAX  = 300
	RAFT_Z_TIME = 250
)

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

type RaftState struct {
	role           Role
	election_timer *time.Timer
	term           int
	cur_vote       *int
	num_votes      int
	leader         *int
}

func NewRaftState(server *rpc.Client, id int) *RaftState {
	s := RaftState{
		role:           RoleFollower,
		election_timer: nil,
		term:           0,
		cur_vote:       nil,
		num_votes:      0,
		leader:         nil,
	}
	s.ResetElectionTimer(server, id)
	return &s
}

// RequestVotes for candidate calling on them
func (s *RaftState) ShouldRequestVote(server *rpc.Client, args shared.RequestVote, id int) bool {
	fmt.Printf("Node %d: Received vote request from %d with term %d\n", id, args.CandidateId, args.Term)
	// fmt.Printf("Recieved vote request from %d with term %d, current %d, votedFor %s\n", args.CandidateId, args.Term, currentTerm, votedFor)
	// Check if the term is valid
	if args.Term < s.term {
		return false
	}

	// If new term, update current term and revert to follower
	if args.Term > s.term {
		s.term = args.Term
		s.role = RoleFollower
		s.cur_vote = nil
	}

	// Skip vote if a candidate or leader
	if s.role == RoleLeader || s.role == RoleCandidate {
		return false
	}

	// If already voted this term
	if s.cur_vote != nil && *s.cur_vote != args.CandidateId {
		return false
	}

	// Grant vote if haven't voted in this term or already voted for this candidate
	fmt.Printf("Node %d: Granted vote to %d for term %d\n", id, args.CandidateId, s.term)
	return true
}

func (s *RaftState) RequestVote(server *rpc.Client, args shared.RequestVote, id int) {
	if s.ShouldRequestVote(server, args, id) {
		s.cur_vote = &args.CandidateId
		s.ResetElectionTimer(server, id)
		shared.SendMessage(server, args.CandidateId, shared.RequestVoteResp{
			Term: s.term,
			Vote: true,
		})
	} else {
		shared.SendMessage(server, args.CandidateId, shared.RequestVoteResp{
			Term: s.term,
			Vote: false,
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
		s.StartElection(server, id)
	})
}

// Start a new election
func (s *RaftState) StartElection(server *rpc.Client, id int) {
	// Only start election if still a follower (or candidate with expired election)
	if s.role == RoleLeader {
		return
	}

	// Increment term and vote for self
	s.term++
	s.cur_vote = &id
	s.role = RoleCandidate
	s.num_votes = 1 // Vote for self

	fmt.Printf("Node %d: Starting election for term %d\n", id, s.term)

	// Request votes from all nodes
	for i := 1; i <= shared.MAX_NODES; i++ {
		if i == id {
			continue // Skip self
		}

		// Send RequestVote to each node
		voteReq := shared.RequestVote{
			Term:        s.term,
			CandidateId: id,
		}
		shared.AsyncSendMessage(server, i, voteReq)
	}

	// Reset election timeout in case we don't get majority
	s.ResetElectionTimer(server, id)
}

// Handle vote responses
func (s *RaftState) VoteResponse(resp shared.RequestVoteResp, id int) {
	// If received a higher term, revert to follower
	if resp.Term > s.term {
		s.term = resp.Term
		s.role = RoleFollower
		s.cur_vote = nil
		s.num_votes = 0 // Reset votes
		return
	}

	// fmt.Printf("Node %d: Received vote response for term %d role %s and vote %s\n", self_node.ID, resp.Term, role, resp.Vote)

	// Only count votes if still a candidate
	if s.role == RoleCandidate && s.term == resp.Term && resp.Vote {
		s.num_votes++

		fmt.Printf("Node %d: Total votes: %d\n", id, s.num_votes)

		// If we have majority, become leader
		if s.num_votes > shared.MAX_NODES/2 {
			// if votes == 2 {
			if s.role != RoleCandidate {
				return
			}

			s.role = RoleLeader
			fmt.Printf("Node %d: Became leader for term %d\n", id, s.term)

			// Stop election timer as leaders don't timeout
			if s.election_timer != nil {
				s.election_timer.Stop()
			}
		}
	}
}

// Send leader heartbeat to all nodes
func (s *RaftState) SendHeartbeats(server *rpc.Client, id int) {
	if s.role != RoleLeader {
		return
	}

	for i := 1; i <= shared.MAX_NODES; i++ {
		if i == id {
			continue // Skip self
		}

		heartbeat := shared.LeaderHeartbeat{
			Term:     s.term,
			LeaderId: id,
		}
		shared.SendMessage(server, i, heartbeat)
	}
}

// Handle leader heartbeats
func (s *RaftState) HandleLeaderHeartbeat(server *rpc.Client, heartbeat shared.LeaderHeartbeat, id int) {
	// If term is outdated, ignore
	if heartbeat.Term < s.term {
		return
	}

	// If new term or valid heartbeat from current term
	if heartbeat.Term >= s.term {
		// Update term if needed
		if heartbeat.Term > s.term {
			s.term = heartbeat.Term
			s.cur_vote = nil
		}

		// Reset to follower (even if already follower)
		s.role = RoleFollower

		// Assign as current leader
		s.leader = &heartbeat.LeaderId

		// Reset election timer
		s.ResetElectionTimer(server, id)

		// fmt.Printf("Node %d: Received heartbeat from leader %d (term %d)\n",
		// 	id, heartbeat.LeaderId, heartbeat.Term)
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
