package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"strings"

	"github.com/k1-23/raft/src/raft/types"
)

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

const ErrNotLeader = "not leader should redirect to this :"

func getPort() string {
	var port string

	flag.StringVar(&port, "port", "", "Port (required)")

	flag.Parse()

	// Validate required flags
	if port == "" {
		log.Fatal("Missing required parameter: --port $port")
	}

	return port
}

func dialServer(port string) (*rpc.Client, error) {
	return rpc.DialHTTP("tcp", "localhost:"+port)
}

func extractPort(errMsg string) (string, error) {
	re := regexp.MustCompile(`:([0-9]+)$`)
	matches := re.FindStringSubmatch(errMsg)
	if len(matches) == 2 {
		return matches[1], nil
	}
	return "", errors.New("could not extract port from error message")
}

func main() {
	port := getPort()
	client, err := dialServer(port)
	if err != nil {
		log.Fatalf("Error in dialing: %s", err)
	}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalf("Error reading input: %s", err)
		}

		input = strings.TrimSpace(input)
		if input == "exit" {
			break
		}

		parts := strings.Split(input, "(")
		if len(parts) == 0 {
			fmt.Println("Unknown command. Use: ping, set(key,value), get(key), del(key), strln(key), append(key), exit")
			continue
		}

		command := parts[0]
		var args []string
		if command != "ping" && command != "request_log" {
			if len(parts) != 2 {
				fmt.Println("Unknown command. Use: ping, set(key,value), get(key), del(key), strln(key), append(key), exit")
				continue
			}
			params := strings.TrimRight(parts[1], ")")
			args = strings.Split(params, ",")
		}

		var reply string
		var logs []types.Entry
		switch command {
		case "ping":
			cmd := types.Command{Kind: types.PingCommand}
			err = client.Call("RaftRPC.Ping", &cmd, &reply)
		case "set":
			if len(args) != 2 {
				fmt.Println("Invalid parameters for set. Use: set(key,value)")
				continue
			}
			cmd := types.Command{Kind: types.SetCommand, Key: args[0], Value: args[1]}
			err = client.Call("RaftRPC.Set", &cmd, &reply)
		case "get":
			if len(args) != 1 {
				fmt.Println("Invalid parameters for get. Use: get(key)")
				continue
			}
			cmd := types.Command{Kind: types.GetCommand, Key: args[0]}
			err = client.Call("RaftRPC.Get", &cmd, &reply)
		case "del":
			if len(args) != 1 {
				fmt.Println("Invalid parameters for del. Use: del(key)")
				continue
			}
			cmd := types.Command{Kind: types.DelCommand, Key: args[0]}
			err = client.Call("RaftRPC.Delete", &cmd, &reply)
		case "strln":
			if len(args) != 1 {
				fmt.Println("Invalid parameters for strln. Use: strln(key)")
				continue
			}
			cmd := types.Command{Kind: types.StrlnCommand, Key: args[0]}
			err = client.Call("RaftRPC.Strln", &cmd, &reply)
		case "append":
			if len(args) != 2 {
				fmt.Println("Invalid parameters for append. Use: append(key,value)")
				continue
			}
			cmd := types.Command{Kind: types.AppendCommand, Key: args[0], Value: args[1]}
			err = client.Call("RaftRPC.Append", &cmd, &reply)
		case "request_log":
			cmd := types.Command{Kind: types.ReqLogCommand}
			err = client.Call("RaftRPC.RequestLog", &cmd, &logs)
		default:
			fmt.Println("Unknown command. Use: ping, set(key,value), get(key), del(key), strln(key), append(key), exit")
			continue
		}

		if err != nil {
			if strings.Contains(err.Error(), ErrNotLeader) {
				port, parseErr := extractPort(err.Error())
				if parseErr != nil {
					log.Fatalf("Error extracting port from error message: %s", parseErr)
				}
				fmt.Println("Redirected to leader at port:", port)
				client, err = dialServer(port)
				if err != nil {
					log.Fatalf("Error in dialing leader: %s", err)
				}
			} else {
				log.Fatalf("Error in RaftRPC.%s: %s", strings.Title(command), err)
			}
		} else {
			if command == "request_log" {
				fmt.Printf("Logs:\n")
				for _, entry := range logs {
					c := decodeCommand(entry.Command)
					var command string
					if c.Kind == types.SetCommand {
						command = "Set"
					}
					if c.Kind == types.GetCommand {
						command = "Get"
					}
					if c.Kind == types.DelCommand {
						command = "Del"
					}
					if c.Kind == types.AppendCommand {
						command = "Append"
					}
					if c.Kind == types.StrlnCommand {
						command = "Strln"
					}
					fmt.Printf("{ Command Kind: %s, Key: %s, Value: %s, Term: %d  Result: success },\n", command, c.Key, c.Value, entry.Term)
				}
			} else {
				fmt.Println(reply)
			}
		}
	}
}
