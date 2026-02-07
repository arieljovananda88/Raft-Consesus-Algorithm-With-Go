package raft

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/k1-23/raft/src/raft/types"
)

func (s *Server) AppendEntries() {
	s.MarkInactiveInCluster()
	for i := range s.Cluster {
		if i == s.IndexInCluster {
			continue
		}

		if !s.Cluster[i].Active {
			continue
		}

		go func(index int) {
			s.Mu.Lock()

			next := s.Cluster[index].NextLogIndex

			// prevLogIndex is next-1, or 0 if next == 0
			var prevLogIndex uint64
			var prevLogTerm uint64

			if next > 0 {
				prevLogIndex = next - 1
			} else {
				prevLogIndex = 0
			}

			// prevLogTerm comes from the log if prevLogIndex > 0
			if prevLogIndex > 0 && prevLogIndex-1 < uint64(len(s.Log)) {
				prevLogTerm = s.Log[prevLogIndex-1].Term
			} else {
				prevLogTerm = 0
			}

			// Entries are everything from next onward
			var entriesToAppend []types.Entry
			if next > 0 && next-1 < uint64(len(s.Log)) {
				entriesToAppend = s.Log[next-1:]
			}

			// Limit batch size
			if len(entriesToAppend) > MAX_ENTRIES_PER_APPEND {
				entriesToAppend = entriesToAppend[:MAX_ENTRIES_PER_APPEND]
			}

			req := types.AppendEntriesRequest{
				Term:         s.CurrentTerm,
				LeaderID:     s.Cluster[s.IndexInCluster].Id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entriesToAppend,
				LeaderCommit: s.CommitIndex,
			}
			s.Mu.Unlock()
			var res types.AppendEntriesResponse
			ok := s.rpcCall(index, "Server.HandleAppendEntriesRequest", req, &res)
			if !ok {
				return
			}

			s.Mu.Lock()
			defer s.Mu.Unlock()
			if res.Success {
				s.Cluster[index].NextLogIndex = max(req.PrevLogIndex+uint64(len(entriesToAppend))+1, 1)
				s.Cluster[index].MatchIndex = s.Cluster[index].NextLogIndex - 1
			} else {
				s.Cluster[index].NextLogIndex = max(s.Cluster[index].NextLogIndex-1, 1)
			}

		}(i)
	}
}

func (s *Server) HandleAppendEntriesRequest(req types.AppendEntriesRequest, rsp *types.AppendEntriesResponse) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	s.updateTerm(req.Term)
	rsp.Term = s.CurrentTerm
	rsp.Success = false

	// s.debug("received heartbeat")
	if req.Term == s.CurrentTerm && s.Membership == types.Candidate {
		s.Membership = types.Follower
	}

	if s.Membership != types.Follower {
		s.debugf("Non-follower cannot append entries.")
		return nil
	}

	if req.Term < s.CurrentTerm {
		s.debugf("Dropping request from old leader %d: term %d.", req.LeaderID, req.Term)
		return nil
	}

	s.debug("[Follower] Heartbeat Received")
	interval := time.Duration(rand.Intn(s.HeartbeatInterval*2) + s.HeartbeatInterval*2)
	s.ElectionTimeout = time.Now().Add(interval * time.Millisecond)

	logLen := uint64(len(s.Log))
	validPreviousLog := req.PrevLogIndex == 0 ||
		(req.PrevLogIndex-1 < logLen &&
			s.Log[req.PrevLogIndex-1].Term == req.PrevLogTerm)
	if !validPreviousLog {
		s.debug("Not a valid log.")
		return nil
	}

	next := req.PrevLogIndex
	nNewEntries := 0

	for i := next; i < next+uint64(len(req.Entries)); i++ {
		e := req.Entries[i-next]

		if i >= uint64(cap(s.Log)) {
			newTotal := next + uint64(len(req.Entries))
			newLog := make([]types.Entry, i, newTotal*2)
			copy(newLog, s.Log)
			s.Log = newLog
		}

		if i < uint64(len(s.Log)) && s.Log[i].Term != e.Term {
			s.Log = s.Log[:i]
		}

		if i < uint64(len(s.Log)) {
			s.debug("Existing Log is the same as new Log")
		} else {
			s.debugf("Appending entry: %s. At index: %d.", string(e.Command), len(s.Log))
			s.Log = append(s.Log, e)
			nNewEntries++
		}
	}

	if req.LeaderCommit > s.CommitIndex {
		s.CommitIndex = min(req.LeaderCommit, uint64(len(s.Log)-1))
	}

	rsp.Success = true
	return nil
}

func (s *Server) sendHeartbeat() {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	send := time.Now().After(s.NextHeartbeat)
	if send {
		s.NextHeartbeat = time.Now().Add(time.Duration(s.HeartbeatInterval) * time.Millisecond)
		s.debug("[Leader] Sending heartbeat to followers")
		s.AppendEntries()
	}
}

func (s *Server) advanceCommitIndex() {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if len(s.Log) == 0 {
		return
	}

	if s.Membership == types.Leader {
		s.updateCommitIndexAsLeader()
	}

	if s.LastApplied <= s.CommitIndex {
		s.commitLogEntry()
	}
}

func (s *Server) updateCommitIndexAsLeader() {
	lastLogIndex := uint64(len(s.Log) - 1)
	for i := lastLogIndex; i > s.CommitIndex; i-- {
		if s.hasQuorumForIndex(i) {
			s.CommitIndex = i
			s.debugf("New commit index: %d.", i)
			break
		}
	}
}

func (s *Server) hasQuorumForIndex(index uint64) bool {
	active := 0
	matched := 0

	for i := range s.Cluster {
		if s.Cluster[i].Active {
			active++
			if i == s.IndexInCluster || s.Cluster[i].MatchIndex >= index {
				matched++
			}
		}
	}

	return matched >= active/2+1
}

func (s *Server) commitLogEntry() {
	logEntry := s.Log[s.LastApplied]

	if len(logEntry.Command) > 0 {
		s.debugf("Entry applied: %d.", s.LastApplied)
		fmt.Println(logEntry.Command)
		res, err := s.Statemachine.CommitCommand(logEntry.Command)
		s.sendResult(logEntry, res, err)
	}

	s.LastApplied++
}

func (s *Server) sendResult(logEntry types.Entry, res []byte, err error) {
	if logEntry.Result != nil {
		logEntry.Result <- types.AddLogResult{
			Result: res,
			Error:  err,
		}
	}
}
