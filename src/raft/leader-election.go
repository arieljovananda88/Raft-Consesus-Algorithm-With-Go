package raft

import (
	"math/rand"
	"time"

	"github.com/k1-23/raft/src/raft/types"
)

func (s *Server) timeout() {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	timedOut := time.Now().After(s.ElectionTimeout)
	if timedOut {
		interval := time.Duration(rand.Intn(s.HeartbeatInterval*2) + s.HeartbeatInterval*2)
		s.debugf("Heartbeat timed out, becoming candidate")
		s.Membership = types.Candidate

		s.CurrentTerm++
		for i := range s.Cluster {
			if i == s.IndexInCluster {
				s.Cluster[i].VotedFor = s.Id
			} else {
				s.Cluster[i].VotedFor = 0
			}
		}
		s.ElectionTimeout = time.Now().Add(interval * time.Millisecond)
		s.requestVote()
	}
}

func (s *Server) requestVote() {
	s.MarkInactiveInCluster()
	for i := range s.Cluster {
		s.debugf("[Candidate] Requesting Votes")
		if i == s.IndexInCluster {
			continue 
		}
		if !s.Cluster[i].Active {
			continue
		}

		go func(index int) {
			var logLen uint64
			if len(s.Log) > 0 {
				logLen = s.Log[len(s.Log)-1].Term
			} else {
				logLen = 0
			}
			req := types.RequestVoteRequest{
				Term:         s.CurrentTerm,
				CandidateID:  s.Id,
				LastLogIndex: uint64(len(s.Log)),
				LastLogTerm:  logLen,
			}
			var res types.RequestVoteResponse

			// Send RPC to server at index
			ok := s.rpcCall(index, "Server.HandleRequestVoteRequest", req, &res)
			if !ok {
				return
			}

			s.Mu.Lock()
			defer s.Mu.Unlock()
			if res.Term > s.CurrentTerm {
				s.updateTerm(res.Term)
			} else if res.VoteGranted {
				s.Cluster[index].VotedFor = s.Id
			}
		}(i)
	}
}

func (s *Server) HandleRequestVoteRequest(req types.RequestVoteRequest, rsp *types.RequestVoteResponse) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if req.Term > s.CurrentTerm {
		s.updateTerm(req.Term)
	}

	if req.Term < s.CurrentTerm ||
		(req.Term == s.CurrentTerm && req.LastLogIndex < uint64(len(s.Log))) {
		rsp.Term = s.CurrentTerm
		rsp.VoteGranted = false
		return nil
	}

	if s.Cluster[s.IndexInCluster].VotedFor == 0 {
		interval := time.Duration(rand.Intn(s.HeartbeatInterval*2) + s.HeartbeatInterval*2)
		s.ElectionTimeout = time.Now().Add(interval * time.Millisecond)
		s.Cluster[s.IndexInCluster].VotedFor = req.CandidateID
		s.debugf("Giving vote to %d", req.CandidateID)
		rsp.Term = s.CurrentTerm
		rsp.VoteGranted = true
		return nil
	}

	rsp.Term = s.CurrentTerm
	rsp.VoteGranted = false
	return nil
}

func (s *Server) becomeLeader() {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	votesReceived := 1 


	for i := range s.Cluster {
		if i != s.IndexInCluster {
			if s.Cluster[i].VotedFor == uint64(s.Id) {
				votesReceived++
			}
		}
	}

	var activeMembers []types.ClusterMember
	for _, member := range s.Cluster {
		if member.Active {
			activeMembers = append(activeMembers, member)
		}
	}

	quorum := len(activeMembers)/2 + 1
	if votesReceived >= quorum {
		s.debug("[Leader] Candidate became leader")
		s.UpdateMemberLeaderInfo()
		s.Membership = types.Leader
	}
}
