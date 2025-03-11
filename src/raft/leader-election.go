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
		// s.debugf("New interval: %s.", interval*time.Millisecond)
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
		// fmt.Println("cluster len")
		// fmt.Println(len(s.Cluster))
		s.debugf("[Candidate] Requesting Votes")
		if i == s.IndexInCluster {
			continue // Skip sending RPC to self
		}
		if !s.Cluster[i].Active {
			continue
		}

		go func(index int) {
			var logLen uint64
			// Construct request vote RPC
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

			// Process response
			s.Mu.Lock()
			defer s.Mu.Unlock()
			if res.Term > s.CurrentTerm {
				s.updateTerm(res.Term)
			} else if res.VoteGranted {
				// Received vote from server at index
				s.Cluster[index].VotedFor = s.Id
			}
		}(i)
	}
}

func (s *Server) HandleRequestVoteRequest(req types.RequestVoteRequest, rsp *types.RequestVoteResponse) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	// fmt.Println("masuk")
	// fmt.Println(s.Cluster[s.IndexInCluster].VotedFor)

	// Check if the candidate's term is newer
	if req.Term > s.CurrentTerm {
		// fmt.Println("tes")
		s.updateTerm(req.Term)
	}

	// Check if the candidate's log is at least as up-to-date as ours
	if req.Term < s.CurrentTerm ||
		(req.Term == s.CurrentTerm && req.LastLogIndex < uint64(len(s.Log))) {
		// fmt.Println("tes1")
		rsp.Term = s.CurrentTerm
		rsp.VoteGranted = false
		return nil
	}

	// If we haven't voted in this term and candidate's log is up-to-date, grant vote
	if s.Cluster[s.IndexInCluster].VotedFor == 0 {
		interval := time.Duration(rand.Intn(s.HeartbeatInterval*2) + s.HeartbeatInterval*2)
		s.ElectionTimeout = time.Now().Add(interval * time.Millisecond)
		s.Cluster[s.IndexInCluster].VotedFor = req.CandidateID
		// fmt.Println(s.Cluster[s.IndexInCluster].VotedFor)
		s.debugf("Giving vote to %d", req.CandidateID)
		rsp.Term = s.CurrentTerm
		rsp.VoteGranted = true
		return nil
	}

	// Reject vote if we have already voted in this term
	rsp.Term = s.CurrentTerm
	rsp.VoteGranted = false
	return nil
}

func (s *Server) becomeLeader() {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	votesReceived := 1 // Counting the vote from the candidate itself

	// Iterate over the cluster to count votes
	for i := range s.Cluster {
		if i != s.IndexInCluster {
			// Check if this node has voted for the current candidate
			if s.Cluster[i].VotedFor == uint64(s.Id) {
				// fmt.Println(s.Cluster[i].VotedFor)
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

	// Calculate the quorum based on active members
	quorum := len(activeMembers)/2 + 1
	// fmt.Println(quorum)
	if votesReceived >= quorum {
		// Transition to the leader state
		// var ok bool
		s.debug("[Leader] Candidate became leader")
		s.UpdateMemberLeaderInfo()
		// s.IndexInCluster, ok = s.FindClusterMemberIndex(s.Id)
		// if !ok {
		// 	s.debugf("error in finding cluster member index in become leader")
		// }
		// s.debugf("new index in cluster: %d", s.IndexInCluster)
		s.Membership = types.Leader
		// fmt.Println(s.Membership)
	}
}
