package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"net/rpc"
	"time"

	"github.com/k1-23/raft/src/raft/types"
)

func (s *Server) rpcCall(index int, name string, req, rsp any) bool {
	s.Mu.Lock()
	c := s.Cluster[index]
	var err error
	var rpcClient *rpc.Client = c.RpcClient
	if c.RpcClient == nil {
		c.RpcClient, err = rpc.DialHTTP("tcp", c.Address)
		rpcClient = c.RpcClient
	}
	s.Mu.Unlock()

	if err != nil {
		s.debugmsg("Error on rpcCall: failed to dial http")
		return false
	}

	err = rpcClient.Call(name, req, rsp)

	if err != nil {
		// fmt.Println(err)
		s.debugf("Error on rpcCall: failed on call method: %s", err)
		return false
	}

	return true // no errors
}

func (s *Server) AddClusterMember(member types.ClusterMember) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if member.Id == 0 {
		panic("Id must not be 0.")
	}

	s.Cluster = append(s.Cluster, member)
}

func (s *Server) JoinCluster(leaderId uint64, leaderAddress string) error {
	req := types.JoinClusterRequest{
		Id:      s.Id,
		Address: s.Address,
	}
	var res types.JoinClusterResponse

	RpcClient, err := rpc.DialHTTP("tcp", leaderAddress)
	if err != nil {
		s.debugmsg("Error on join cluster: failed to dial http")
		return err
	}
	s.debug("trying to join cluster")
	err = RpcClient.Call("Server.HandleJoinClusterRequest", req, &res)
	// fmt.Println(err)
	if err != nil {
		s.debugmsg("Error on join cluster: failed to call client")
		return err
	}

	if res.Success {
		var leaderCluster []types.ClusterMember
		for _, member := range res.JoinedCluster {
			clusterMember := types.ClusterMember{
				Id:           member.Id,
				Address:      member.Address,
				NextLogIndex: member.NextLogIndex,
				MatchIndex:   member.MatchIndex,
				VotedFor:     member.VotedFor,
				Active:       member.Active,
				RpcClient:    nil, // You can dial the actual RPCClient here if needed
			}
			leaderCluster = append(leaderCluster, clusterMember)
		}
		s.Cluster = leaderCluster
		s.IndexInCluster = res.IndexInCluster
		s.LeaderHttpPort = res.LeaderHttpPort
		s.debugf("index on leader cluster: %d", res.IndexInCluster)
		// fmt.Println(s.Cluster)
		return nil
	} else {
		// Failed to join the cluster
		if res.LeaderAddress != "" {
			var notLeaderErr = "node is not leader, join node leader on " + res.LeaderAddress
			return errors.New(notLeaderErr)
		}
		return errors.New("failed to join the cluster")
	}
}

func (s *Server) HandleJoinClusterRequest(req types.JoinClusterRequest, rsp *types.JoinClusterResponse) error {
	// Add the new server to the cluster
	s.Mu.Lock()
	defer s.Mu.Unlock()
	var alreadyExists bool
	var memberReq types.ClusterMemberForSerialization
	var indexInCluster int
	if s.Membership != types.Leader {
		rsp.LeaderAddress = s.LeaderAddress
		rsp.Success = false
		return nil
	}
	for i, member := range s.Cluster {
		if member.Id == req.Id {
			// ID already exists, return an error
			s.debug("id already exists in cluster")
			alreadyExists = true
			indexInCluster = i
		}
	}
	if !alreadyExists {
		s.debug("appending new server to cluster")
		member := types.ClusterMember{Id: req.Id, Address: req.Address, Active: true}
		memberReq = types.ClusterMemberForSerialization{
			Id:      req.Id,
			Address: req.Address,
		}
		s.Cluster = append(s.Cluster, member)
	}
	var serializedCluster []types.ClusterMemberForSerialization
	for i, member := range s.Cluster {
		serializedCluster = append(serializedCluster, types.ClusterMemberForSerialization{
			Id:           member.Id,
			Address:      member.Address,
			NextLogIndex: member.NextLogIndex,
			MatchIndex:   member.MatchIndex,
			VotedFor:     member.VotedFor,
			Active:       member.Active,
		})
		if alreadyExists {
			continue
		}
		if i == s.IndexInCluster || i == len(s.Cluster)-1 {
			s.debugf("skipping")
			continue
		}
		if !s.Cluster[i].Active {
			continue
		} else {
			var rsp types.AppendNewMemberResponse
			// fmt.Println(s.Cluster[i].Address)
			RpcClient, err := rpc.DialHTTP("tcp", s.Cluster[i].Address)
			if err != nil {
				s.debugmsg("Error on join cluster: failed to dial http")
				return err
			}
			err = RpcClient.Call("Server.HandleAppendNewMember", memberReq, &rsp)
			if err != nil {
				s.debugmsg("Error on join cluster: failed to call client")
				return err
			}
			if !rsp.Success {
				s.debugf("error in appending new member to follower clusters")
			}
		}
	}
	if alreadyExists {
		rsp.IndexInCluster = indexInCluster
		// fmt.Println(indexInCluster)
	} else {
		rsp.IndexInCluster = len(s.Cluster) - 1
	}
	rsp.LeaderAddress = ""
	rsp.LeaderHttpPort = s.HttpPort
	rsp.JoinedCluster = serializedCluster
	rsp.Success = true

	return nil
}

func (s *Server) HandleAppendNewMember(req types.ClusterMemberForSerialization, rsp *types.AppendNewMemberResponse) error {
	s.debugf("appending new member to cluster")
	s.Mu.Lock()
	defer s.Mu.Unlock()
	newMember := types.ClusterMember{
		Id:      req.Id,
		Address: req.Address,
		Active:  true,
	}
	s.Cluster = append(s.Cluster, newMember)
	// fmt.Println(s.Cluster)
	rsp.Success = true
	return nil
}

func (s *Server) RemoveClusterMember(id uint64) {
	var ok bool
	s.debug("removing dead server from cluster")
	for i, member := range s.Cluster {
		if member.Id == id {
			s.Cluster = append(s.Cluster[:i], s.Cluster[i+1:]...)
			break
		}
	}
	fmt.Println("new cluster length: ", len(s.Cluster))
	fmt.Println(s.Cluster)
	s.IndexInCluster, ok = s.FindClusterMemberIndex(s.Id)
	if !ok {
		s.debugf("error in finding cluster member index in become leader")
	}
	s.debugf("new index in cluster: %d", s.IndexInCluster)
}

func (s *Server) MarkInactiveMember(id uint64) {
	// var ok bool
	// s.debug("marking member inactive in cluster")
	for i, member := range s.Cluster {
		if member.Id == id {
			s.Cluster[i].Active = false
			// fmt.Println("Inactive member: ", s.Cluster[i])
			break
		}
	}

}

func (s *Server) MarkActiveMember(id uint64) {
	// var ok bool
	// s.debug("marking member inactive in cluster")
	for i, member := range s.Cluster {
		if member.Id == id {
			s.Cluster[i].Active = true
			// fmt.Println("Inactive member: ", s.Cluster[i])
			break
		}
	}

}

func (s *Server) updateTerm(currentTerm uint64) bool {
	if currentTerm > s.CurrentTerm {
		s.CurrentTerm = currentTerm
		s.Membership = types.Follower
		s.Cluster[s.IndexInCluster].VotedFor = 0
		// fmt.Println(s.Cluster[s.IndexInCluster])
		s.debug("Transitioned to follower")
		interval := time.Duration(rand.Intn(s.HeartbeatInterval*2) + s.HeartbeatInterval*2)
		s.debugf("New interval: %s.", interval*time.Millisecond)
		s.ElectionTimeout = time.Now().Add(interval * time.Millisecond)
		return true
	}
	return false
}

func (s *Server) FindClusterMemberIndex(serverID uint64) (int, bool) {
	for i, member := range s.Cluster {
		if member.Id == serverID {
			return i, true
		}
	}
	return -1, false
}

func (s *Server) RemoveDeadServerFromCluster() {
	var inactiveId []uint64
	for i := range s.Cluster {
		_, err := rpc.DialHTTP("tcp", s.Cluster[i].Address)
		if err != nil {
			inactiveId = append(inactiveId, s.Cluster[i].Id)
		}
	}
	for i := range inactiveId {
		s.RemoveClusterMember(inactiveId[i])
	}
}

func (s *Server) MarkInactiveInCluster() {
	var inactiveId []uint64
	for i := range s.Cluster {
		_, err := rpc.DialHTTP("tcp", s.Cluster[i].Address)
		if err != nil {
			inactiveId = append(inactiveId, s.Cluster[i].Id)
		} else {
			if !s.Cluster[i].Active {
				s.Cluster[i].Active = true
			}
		}
	}
	for i := range inactiveId {
		s.MarkInactiveMember(inactiveId[i])
	}
}

func (s *Server) UpdateMemberLeaderInfo() {
	req := types.UpdateLeaderInfoReq{
		LeaderID:       s.Id,
		LeaderAddress:  s.Address,
		LeaderHttpPort: s.HttpPort,
	}
	var res types.UpdateLeaderInfoRes
	for i := range s.Cluster {
		if i == s.IndexInCluster {
			continue
		}
		if !s.Cluster[i].Active {
			continue
		}
		RpcClient, err := rpc.DialHTTP("tcp", s.Cluster[i].Address)
		if err != nil {
			s.debugmsg("Error on update member leader info cluster: failed to dial http")
		}
		err = RpcClient.Call("Server.HandleUpdateMemberLeaderInfo", req, &res)
		if err != nil {
			s.debugmsg("Error on update member leader info cluster: failed to call client")
		}
		if !res.Success {
			s.debugf("error in update new member to follower clusters")
		}
	}
}

func (s *Server) HandleUpdateMemberLeaderInfo(req types.UpdateLeaderInfoReq, res *types.UpdateLeaderInfoRes) error {
	s.LeaderAddress = req.LeaderAddress
	s.LeaderId = req.LeaderID
	s.LeaderHttpPort = req.LeaderHttpPort
	s.debugf("new leader info %d %s %s", s.LeaderId, s.LeaderAddress, s.LeaderHttpPort)
	res.Success = true
	return nil
}
