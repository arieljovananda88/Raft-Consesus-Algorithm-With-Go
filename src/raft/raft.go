package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/k1-23/raft/src/raft/types"
)

// constants
var MAX_ENTRIES_PER_APPEND = 8

type Server struct {
	Server *http.Server
	Debug  bool

	Mu sync.Mutex

	// The current term
	CurrentTerm uint64

	Log []types.Entry

	LeaderAddress  string
	LeaderHttpPort string
	LeaderId       uint64

	// votedFor is stored in `cluster []ClusterMember` below,
	// mapped by `clusterIndex` below

	// Unique identifier for this Server
	Id uint64

	// The TCP address for RPC
	Address  string
	HttpPort string

	// When to start elections after no append entry messages
	ElectionTimeout time.Time

	// How often to send empty messages
	HeartbeatInterval int

	// When to next send empty message
	NextHeartbeat time.Time

	// User-provided state machine
	Statemachine types.StateMachine

	// Index of highest log entry known to be committed
	CommitIndex uint64

	// Index of highest log entry applied to state machine
	LastApplied uint64

	// Candidate, follower, or leader
	Membership types.MembershipState

	// Servers in the cluster, including this one
	Cluster []types.ClusterMember

	// Index of this server
	IndexInCluster int
}

func NewServer(http string, clusterConfig []types.ClusterMember, statemachine types.StateMachine, indexInCluster int, leaderId uint64, leaderAddress string) *Server {
	var cluster []types.ClusterMember
	var member types.MembershipState
	var leaderHttpPort string
	for _, c := range clusterConfig {
		if c.Id == 0 {
			panic("Id must not be 0.")
		}
		cluster = append(cluster, c)
	}
	if leaderAddress == "" {
		member = types.Leader
		indexInCluster = 0
		leaderId = 0
		leaderAddress = cluster[0].Address
		leaderHttpPort = http
		cluster[0].Active = true
	} else {
		member = types.Follower
		leaderHttpPort = ""
	}
	// ^ for debuggin purposes

	return &Server{
		Id:                cluster[0].Id,
		HttpPort:          http,
		Address:           cluster[0].Address,
		Cluster:           cluster,
		Statemachine:      statemachine,
		IndexInCluster:    indexInCluster,
		HeartbeatInterval: 500,
		Mu:                sync.Mutex{},
		Membership:        member,
		LeaderId:          leaderId,
		LeaderAddress:     leaderAddress,
		LeaderHttpPort:    leaderHttpPort,
	}
}

func (s *Server) AddLog(cmds [][]byte) ([]types.AddLogResult, types.NotLeaderErr) {
	var errLeader = "node should be leader to add logs"
	var notLeader types.NotLeaderErr
	if s.Membership != types.Leader {
		notLeader = types.NotLeaderErr{
			LeaderPort: s.LeaderHttpPort,
			Err:        errors.New(errLeader),
		}
		return nil, notLeader
	}
	s.Mu.Lock()
	resultChans := make([]chan types.AddLogResult, len(cmds))
	for i, command := range cmds {
		resultChans[i] = make(chan types.AddLogResult)
		// fmt.Println(string(command))
		s.Log = append(s.Log, types.Entry{
			Term:    s.CurrentTerm,
			Command: command,
			Result:  resultChans[i],
		})
	}

	s.debug("Waiting to be applied!")
	s.Mu.Unlock()

	s.AppendEntries()

	results := make([]types.AddLogResult, len(cmds))
	var wg sync.WaitGroup
	wg.Add(len(cmds))
	for i, ch := range resultChans {
		go func(i int, c chan types.AddLogResult) {
			results[i] = <-c
			wg.Done()
		}(i, ch)
	}

	wg.Wait()

	notLeader = types.NotLeaderErr{
		LeaderPort: "",
		Err:        nil,
	}
	return results, notLeader
}

func (s *Server) GiveLogs() []types.Entry {
	return s.Log
}

func (s *Server) debugmsg(msg string) string {
	return fmt.Sprintf("%s [Id: %d, Term: %d] %s", time.Now().Format(time.RFC3339Nano), s.Id, s.CurrentTerm, msg)
}

func (s *Server) debug(msg string) {
	fmt.Println(s.debugmsg(msg))
}

func (s *Server) debugf(msg string, args ...any) {
	s.debug(fmt.Sprintf(msg, args...))
}

func (s *Server) Start() {
	rpcServer := rpc.NewServer()
	rpcServer.Register(s)
	l, err := net.Listen("tcp", s.Address)
	if err != nil {
		panic(err)
	}
	if s.Membership == types.Follower {
		err := s.JoinCluster(s.LeaderId, s.LeaderAddress)
		if err != nil {
			panic(err)
		}
	}

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, rpcServer)

	s.Server = &http.Server{Handler: mux}
	go s.Server.Serve(l)

	go func() {
		s.Mu.Lock()
		interval := time.Duration(rand.Intn(s.HeartbeatInterval*2) + s.HeartbeatInterval*2)
		s.debugf("New interval: %s.", interval*time.Millisecond)
		s.ElectionTimeout = time.Now().Add(interval * time.Millisecond)
		s.Mu.Unlock()

		for {
			// fmt.Println(s.Membership)
			s.Mu.Lock()
			membership := s.Membership
			s.Mu.Unlock()

			// fmt.Println(membership)
			if membership == types.Leader {
				s.sendHeartbeat()
				s.advanceCommitIndex()
			} else if membership == types.Follower {
				s.timeout()
				s.advanceCommitIndex()
			} else if membership == types.Candidate {
				s.timeout()
				s.becomeLeader()
			}
		}
	}()
}
