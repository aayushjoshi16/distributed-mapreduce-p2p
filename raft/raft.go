package raft

import (
	"lab4/shared"
)

type Role int

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader

	RAFT_X_TIME = 1000
	RAFT_Y_MIN = 150
	RAFT_Y_MAX = 300
	RAFT_Z_TIME = 250
)

type GossipHeartbeat struct {
	Membership shared.Membership
}

type RequestVote struct {
	Term        	int
	CandidateId 	int
}

type RequestVoteResp struct {
	Term			int
	Vote 			bool
}

type LeaderHeartbeat struct{
	Term 			int
	LeaderId 		int
}
