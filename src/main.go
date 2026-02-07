package main

import (
	"bytes"
	crypto "crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/k1-23/raft/src/raft"
	"github.com/k1-23/raft/src/raft/types"
)

type stateMachine struct {
	db     *sync.Map
	server int
}

type httpServer struct {
	raft *raft.Server
	db   *sync.Map
}

type config struct {
	cluster       []types.ClusterMember
	index         int
	id            string
	address       string
	http          string
	leaderId      uint64
	leaderAddress string
}

type RaftRPC struct {
	hs *httpServer
}

func encodeCommand(c types.Command) ([]byte, error) {
	msg := bytes.NewBuffer(nil)

	// Write command kind
	if err := msg.WriteByte(uint8(c.Kind)); err != nil {
		return nil, fmt.Errorf("failed to write command kind: %w", err)
	}

	// Write key length
	keyLen := uint64(len(c.Key))
	if err := binary.Write(msg, binary.LittleEndian, keyLen); err != nil {
		return nil, fmt.Errorf("failed to write key length: %w", err)
	}

	// Write key
	if _, err := msg.WriteString(c.Key); err != nil {
		return nil, fmt.Errorf("failed to write key: %w", err)
	}

	// Write value length (only for setCommand)
	if c.Kind == types.SetCommand || c.Kind == types.AppendCommand {
		valLen := uint64(len(c.Value))
		// fmt.Println("masuk")
		if err := binary.Write(msg, binary.LittleEndian, valLen); err != nil {
			return nil, fmt.Errorf("failed to write value length: %w", err)
		}

		// Write value
		if _, err := msg.WriteString(c.Value); err != nil {
			return nil, fmt.Errorf("failed to write value: %w", err)
		}
	}

	return msg.Bytes(), nil
}

func decodeCommand(msg []byte) types.Command {
	var c types.Command

	// Decode command kind
	c.Kind = types.CommandKind(msg[0])

	// Decode key length (8 bytes)
	keyLen := binary.LittleEndian.Uint64(msg[1:9])

	// Decode key
	keyStart := 9
	keyEnd := keyStart + int(keyLen)
	c.Key = string(msg[keyStart:keyEnd])

	// If command is of type setCommand, decode the value
	if c.Kind == types.SetCommand || c.Kind == types.AppendCommand {
		// Decode value length (8 bytes)
		valLenStart := keyEnd
		valLenEnd := valLenStart + 8
		valLen := binary.LittleEndian.Uint64(msg[valLenStart:valLenEnd])

		// Decode value
		valStart := valLenEnd
		valEnd := valStart + int(valLen)
		c.Value = string(msg[valStart:valEnd])
	}

	return c
}

func (s *stateMachine) CommitCommand(cmd []byte) ([]byte, error) {
	c := decodeCommand(cmd)

	if c.Kind == types.SetCommand {
		s.db.Store(c.Key, c.Value)
	} else if c.Kind == types.GetCommand {
		value, ok := s.db.Load(c.Key)
		if !ok {
			return nil, fmt.Errorf("key not found")
		}
		// fmt.Println(value)
		return []byte(value.(string)), nil
	} else if c.Kind == types.StrlnCommand {
		value, ok := s.db.Load(c.Key)
		if !ok {
			return nil, fmt.Errorf("key not found")
		}
		length := len(value.(string))
		return []byte(fmt.Sprintf("%d", length)), nil
	} else if c.Kind == types.DelCommand {
		value, ok := s.db.Load(c.Key)
		if !ok {
			return nil, nil
		}
		s.db.Delete(c.Key)
		return []byte(value.(string)), nil
	} else if c.Kind == types.AppendCommand {
		_, ok := s.db.Load(c.Key)
		if !ok {
			s.db.Store(c.Key, "")
		} else {
			existingValue, _ := s.db.Load(c.Key)
			newValue := existingValue.(string) + c.Value
			// fmt.Println(c.Value)
			// fmt.Println(newValue)
			s.db.Store(c.Key, newValue)
		}
	}

	return nil, fmt.Errorf("unknown command: %x", cmd)
}

// Example:
//
//	curl http://localhost:2020/set?key=x&value=1
func (hs httpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	var c types.Command
	c.Kind = types.SetCommand
	c.Key = r.URL.Query().Get("key")
	c.Value = r.URL.Query().Get("value")
	encodedCommand, err := encodeCommand(c)
	if err != nil {
		fmt.Println("error while encoding command")
	}
	_, notLeader := hs.raft.AddLog([][]byte{encodedCommand})
	if notLeader.Err != nil {
		log.Printf("Could not write key-value: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))

}

func (hs httpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	var c types.Command
	c.Kind = types.GetCommand
	c.Key = r.URL.Query().Get("key")
	var value []byte
	var err error
	var results []types.AddLogResult
	encodedCommand, err := encodeCommand(c)
	if err != nil {
		fmt.Println("error while encoding command")
	}

	// Channel to wait for all results to be received
	done := make(chan struct{})
	defer close(done)

	// Add command to Raft log
	results, notLeader := hs.raft.AddLog([][]byte{encodedCommand})
	if notLeader.Err != nil {
		log.Printf("Could not encode key-value in http response: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	// Wait for all results to be received
	var wg sync.WaitGroup

	wg.Add(len(results))
	go func() {
		defer wg.Done()
		for _, result := range results {
			value = append(value, result.Result...)
		}
	}()

	// Wait until all results are received or timeout occurs
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	// Wait for all results to be received or timeout
	select {
	case <-done:
		// All results received, proceed with sending response
	case <-time.After(time.Second * 5): // Adjust timeout duration as needed
		// Timeout occurred, return error response
		log.Println("Timeout waiting for results")
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	_, err = w.Write(value)
	if err != nil {
		log.Printf("Could not encode key-value in http response: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
}

func (r *RaftRPC) Ping(args *types.Command, reply *string) error {
	*reply = "PONG"
	return nil
}

func (r *RaftRPC) Set(args *types.Command, reply *string) error {
	encodedCommand, err := encodeCommand(*args)
	if err != nil {
		return fmt.Errorf("error while encoding command: %w", err)
	}
	_, notLeader := r.hs.raft.AddLog([][]byte{encodedCommand})
	if notLeader.Err != nil {
		if notLeader.LeaderPort != "" {
			return fmt.Errorf("not leader should redirect to this %s", notLeader.LeaderPort)
		}
		return fmt.Errorf("could not write key-value: %w", err)
	}
	*reply = "OK"
	return nil
}

func (r *RaftRPC) Append(args *types.Command, reply *string) error {
	encodedCommand, err := encodeCommand(*args)
	if err != nil {
		return fmt.Errorf("error while encoding command: %w", err)
	}
	_, notLeader := r.hs.raft.AddLog([][]byte{encodedCommand})
	if notLeader.Err != nil {
		if notLeader.LeaderPort != "" {
			return fmt.Errorf("not leader should redirect to this %s", notLeader.LeaderPort)
		}
		return fmt.Errorf("could not write key-value: %w", err)
	}
	*reply = "OK"
	return nil
}

func (r *RaftRPC) Get(args *types.Command, reply *string) error {
	encodedCommand, err := encodeCommand(*args)
	if err != nil {

		return fmt.Errorf("error while encoding command: %w", err)
	}

	results, notLeader := r.hs.raft.AddLog([][]byte{encodedCommand})
	if notLeader.Err != nil {
		if notLeader.LeaderPort != "" {
			return fmt.Errorf("not leader should redirect to this %s", notLeader.LeaderPort)
		}
		return fmt.Errorf("could not add log: %w", err)
	}

	var value []byte
	for _, result := range results {
		value = append(value, result.Result...)
	}

	*reply = string(value)
	return nil
}

func (r *RaftRPC) Strln(args *types.Command, reply *string) error {
	encodedCommand, err := encodeCommand(*args)
	if err != nil {
		return fmt.Errorf("error while encoding command: %w", err)
	}

	results, notLeader := r.hs.raft.AddLog([][]byte{encodedCommand})
	if notLeader.Err != nil {
		if notLeader.LeaderPort != "" {
			return fmt.Errorf("not leader should redirect to this %s", notLeader.LeaderPort)
		}
		return fmt.Errorf("could not add log: %w", err)
	}

	var value []byte
	for _, result := range results {
		value = append(value, result.Result...)
	}

	*reply = string(value)
	return nil
}

func (r *RaftRPC) Delete(args *types.Command, reply *string) error {
	encodedCommand, err := encodeCommand(*args)
	if err != nil {
		return fmt.Errorf("error while encoding command: %w", err)
	}

	results, notLeader := r.hs.raft.AddLog([][]byte{encodedCommand})
	if notLeader.Err != nil {
		if notLeader.LeaderPort != "" {
			return fmt.Errorf("not leader should redirect to this %s", notLeader.LeaderPort)
		}
		return fmt.Errorf("could not add log: %w", err)
	}

	var value []byte
	for _, result := range results {
		value = append(value, result.Result...)
	}

	*reply = string(value)
	return nil
}

func (r *RaftRPC) RequestLog(args *types.Command, reply *[]types.Entry) error {
	logs := r.hs.raft.GiveLogs()
	*reply = logs
	return nil
}

func getConfig() config {
	var (
		node       string
		http       string
		cluster    string
		leader     string
		leaderId   uint64
		leaderAddr string
	)

	// Define flags
	flag.StringVar(&node, "node", "", "Node index (required)")
	flag.StringVar(&http, "http", "", "HTTP address (required)")
	flag.StringVar(&cluster, "cluster", "", "Cluster members in the format id,address;... (required)")
	flag.StringVar(&leader, "leader", "", "Leader information in the format id,address (optional)")
	flag.Parse()

	// Validate required flags
	if node == "" {
		log.Fatal("Missing required parameter: --node $index")
	}
	if http == "" {
		log.Fatal("Missing required parameter: --http $address")
	}
	if cluster == "" {
		log.Fatal("Missing required parameter: --cluster $node1Id,$node1Address;...;$nodeNId,$nodeNAddress")
	}

	// Parse node index
	index, err := strconv.Atoi(node)
	if err != nil {
		log.Fatalf("Expected $value to be a valid integer in `--node $value`, got: %s", node)
	}

	// Parse cluster members
	var clusterMembers []types.ClusterMember
	for _, part := range strings.Split(cluster, ";") {
		idAddress := strings.Split(part, ",")
		if len(idAddress) != 2 {
			log.Fatalf("Expected cluster member format `--cluster $id,$ip`, got: %s", part)
		}

		idStr, err := strconv.Atoi(idAddress[0])
		if err != nil {
			log.Fatalf("error parsing idaddress[0] into string")
		}
		if index != idStr {
			log.Fatalf("node index and cluster id should be the same, got: %d, want: %d", idStr, index)
		}
		id, err := strconv.ParseUint(idAddress[0], 10, 64)
		if err != nil {
			log.Fatalf("Expected $id to be a valid integer in `--cluster $id,$ip`, got: %s", idAddress[0])
		}

		clusterMembers = append(clusterMembers, types.ClusterMember{
			Id:      id,
			Address: idAddress[1],
		})
	}

	if leader != "" {
		leaderInfo := strings.Split(leader, ",")
		if len(leaderInfo) != 2 {
			log.Fatal("Invalid leader information format. Expected id,address")
		}
		leaderId, err = strconv.ParseUint(leaderInfo[0], 10, 64)
		if err != nil {
			fmt.Println("error parsing leader info to uint64")
			return config{}
		}
		leaderAddr = leaderInfo[1]
	} else {
		leaderId = 0
		leaderAddr = ""
	}

	// Return the parsed configuration
	return config{
		index:         index,
		http:          http,
		cluster:       clusterMembers,
		leaderId:      leaderId,
		leaderAddress: leaderAddr,
	}
}

func main() {
	var b [8]byte
	_, err := crypto.Read(b[:])
	if err != nil {
		panic("cannot seed math/rand package with cryptographically secure random number generator")
	}
	rand.Seed(int64(binary.LittleEndian.Uint64(b[:])))

	cfg := getConfig()

	var db sync.Map

	var sm stateMachine
	sm.db = &db
	sm.server = cfg.index
	s := raft.NewServer(cfg.http, cfg.cluster, &sm, cfg.index, cfg.leaderId, cfg.leaderAddress)
	go s.Start()

	hs := httpServer{s, &db}

	rpcServer := new(RaftRPC)
	rpcServer.hs = &hs

	rpc.Register(rpcServer)
	rpc.HandleHTTP()

	http.HandleFunc("/set", hs.setHandler)
	http.HandleFunc("/get", hs.getHandler)
	err = http.ListenAndServe(cfg.http, nil)
	if err != nil {
		panic(err)
	}
}
