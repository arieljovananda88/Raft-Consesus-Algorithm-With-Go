package types

import (
	"net/rpc"
)

type CommandKind uint8

const (
	SetCommand CommandKind = iota
	GetCommand
	StrlnCommand
	DelCommand
	AppendCommand
	PingCommand
	ReqLogCommand
)

type Command struct {
	Kind  CommandKind
	Key   string
	Value string
}

type StateMachine interface {
	CommitCommand(cmd []byte) ([]byte, error)
}

type AddLogResult struct {
	Result []byte
	Error  error
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

type Entry struct {
	Command []byte
	Term    uint64

	// Set by the primary so it can learn about the result of
	// applying this command to the state machine
	Result chan AddLogResult
}

type ClusterMember struct {
	Id      uint64
	Address string

	// Index of the next log entry to send
	NextLogIndex uint64
	// Highest log entry known to be replicated
	MatchIndex uint64

	// Who was voted for in the most recent term
	VotedFor uint64
	Active   bool

	// TCP connection
	RpcClient *rpc.Client
}

type ClusterMemberForSerialization struct {
	Id      uint64
	Address string

	// Index of the next log entry to send
	NextLogIndex uint64
	// Highest log entry known to be replicated
	MatchIndex uint64

	// Who was voted for in the most recent term
	VotedFor uint64
	Active   bool
}

type MembershipState string

const (
	Leader    MembershipState = "leader"
	Follower  MembershipState = "follower"
	Candidate MembershipState = "candidate"
)

type JoinClusterRequest struct {
	Id      uint64 // ID of the joining server
	Address string // Address of the joining server

}

type JoinClusterResponse struct {
	Success        bool // Indicates whether the join operation was successful
	LeaderAddress  string
	JoinedCluster  []ClusterMemberForSerialization
	IndexInCluster int
	LeaderHttpPort string
}

type RequestVoteRequest struct {
	Term         uint64 // Candidate's term
	CandidateID  uint64 // ID of candidate requesting vote
	LastLogIndex uint64 // Index of candidate’s last log entry
	LastLogTerm  uint64 // Term of candidate’s last log entry
}

// RequestVoteResponse represents a response to a vote request in the Raft consensus algorithm.
type RequestVoteResponse struct {
	Term        uint64 // Current term of the responder
	VoteGranted bool   // True if responder granted vote to the candidate
}

type AppendNewMemberResponse struct {
	Success bool
}

type UpdateLeaderInfoReq struct {
	LeaderID       uint64
	LeaderAddress  string
	LeaderHttpPort string
}

type UpdateLeaderInfoRes struct {
	Success bool
}

type NotLeaderErr struct {
	LeaderPort string
	Err        error
}
