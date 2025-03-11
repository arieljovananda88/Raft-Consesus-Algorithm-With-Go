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
			// s.debugf("Leader: len logs = %d, id = %d, next log = %d, match = %d.", len(s.log), s.cluster[i].Id, s.cluster[i].nextLogIndex, s.cluster[i].matchIndex)
			continue
		}
		// fmt.Println(s.Cluster[i].Active)
		if !s.Cluster[i].Active {
			// fmt.Println("masuk")
			continue
		}

		go func(index int) {
			s.Mu.Lock()
			// fmt.Println("append entry")
			// s.debug("[Leader] Sending Heartbeat")
			var entriesToAppend []types.Entry
			var prevLogIndex uint64
			var prevLogTerm uint64
			// var clusterConnected bool
			next := s.Cluster[index].NextLogIndex
			// // fmt.Println(next)
			// fmt.Println("next")
			// fmt.Println(next)
			if next > 0 {
				prevLogIndex = next - 1
			} else {
				prevLogIndex = 0
			}

			// fmt.Println("len log")
			// fmt.Println(len(s.log))

			if len(s.Log) == 0 {
				prevLogTerm = 0
			} else if len(s.Log) == 1 {
				prevLogTerm = s.Log[0].Term
				if next != 0 {
					if uint64(len(s.Log)) >= next {
						entriesToAppend = s.Log[(next - 1):]
					}

					if len(entriesToAppend) > MAX_ENTRIES_PER_APPEND {
						entriesToAppend = entriesToAppend[:MAX_ENTRIES_PER_APPEND]
					}
				}
			} else {
				if next != 0 {
					if next != 1 {
						prevLogTerm = s.Log[prevLogIndex-1].Term
					} else {
						prevLogTerm = s.Log[0].Term
					}

					if uint64(len(s.Log)-1) >= next-1 {
						entriesToAppend = s.Log[(next - 1):]
					}

					if len(entriesToAppend) > MAX_ENTRIES_PER_APPEND {
						entriesToAppend = entriesToAppend[:MAX_ENTRIES_PER_APPEND]
					}
				}
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
				// prev := s.cluster[index].nextLogIndex - 1
				s.Cluster[index].MatchIndex = s.Cluster[index].NextLogIndex - 1
				// s.debugf("Messages (%d) accepted for %d. Prev Index: %d, Next Index: %d, Match Index: %d.", len(req.Entries), s.cluster[index].Id, prev, s.cluster[index].nextLogIndex, s.cluster[index].matchIndex)
			} else {
				s.Cluster[index].NextLogIndex = max(s.Cluster[index].NextLogIndex-1, 1)
				// s.debugf("Forced to go back to %d for: %d.", s.cluster[index].nextLogIndex, s.cluster[index].Id)
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
	// s.debugf("New interval: %s.", interval*time.Millisecond)
	s.ElectionTimeout = time.Now().Add(interval * time.Millisecond)

	logLen := uint64(len(s.Log))
	validPreviousLog := req.PrevLogIndex == 0 /* This is the induction step */ ||
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
			// prevCap := cap(s.Log)
			s.Log = s.Log[:i]
			// Server_assert(s, "Capacity remains the same while we truncated.", cap(s.Log), prevCap)
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
		// fmt.Println(s.commitIndex)
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
	quorum := len(s.Cluster)/2 + 1
	for j := range s.Cluster {
		if quorum == 0 {
			return true
		}
		if j == s.IndexInCluster || s.Cluster[j].MatchIndex >= index {
			quorum--
		}
	}
	return quorum == 0
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
