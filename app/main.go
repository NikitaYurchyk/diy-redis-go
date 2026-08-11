package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"
)

func handleClient(conn net.Conn, store *Store) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	watched := make(map[string]uint64)
	handler := CommandHandler{store: store, watched: watched}

	defer func() {
		fmt.Printf("Client disconnected: %s\n", conn.RemoteAddr())
	}()

	for {
		parts, err := ParseArray(reader)
		if err != nil {
			if err.Error() != "EOF" {
				fmt.Printf("Error received %s: %v\n", conn.LocalAddr(), err)
			}
			return
		}
		
		cmd := ParseCommand(parts)

		if _, err := writer.WriteString(handler.Handle(cmd)); err != nil {
			fmt.Printf("Error writing to %s: %v\n", conn.RemoteAddr(), err)
			return
		}

		if err := writer.Flush(); err != nil {
			fmt.Printf("Error flushing to %s: %v\n", conn.RemoteAddr(), err)
			return
		}

		if _, ok := cmd.(Psync); ok {
			store.AddReplica(conn, handler.replicaPort)
		}
	}
}

func handleReplica(masterHost string, masterPort, listeningPort int) {
	addr := net.JoinHostPort(masterHost, strconv.Itoa(masterPort))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Error connecting to master %s: %v\n", addr, err)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	steps := [][]string{
		{"PING"},
		{"REPLCONF", "listening-port", strconv.Itoa(listeningPort)},
		{"REPLCONF", "capa", "psync2"},
		{"PSYNC", "?", "-1"},
	}

	for _, step := range steps {
		if _, err := conn.Write([]byte(buildArray(step))); err != nil {
			fmt.Printf("Error sending %s to master: %v\n", step[0], err)
			return
		}

		reply, err := readLine(reader)
		if err != nil {
			fmt.Printf("Error reading %s reply: %v\n", step[0], err)
			return
		}
		fmt.Printf("Master replied to %s: %s\n", step[0], reply)
	}

}
func main() {
	port := flag.Int("port", 6379, "port to listen on")
	replicaOf := flag.String("replicaof", "", "master to replicate, as \"<host> <port>\"")
	flag.Parse()
	addr := fmt.Sprintf(":%d", *port)

	listener, err := net.Listen("tcp", addr)

	if err != nil {
		panic(err)
	}

	defer listener.Close()

	store := NewStore()
	store.info = InitInfo()
	if *replicaOf != "" {
		fields := strings.Fields(*replicaOf)
		if len(fields) != 2 {
			panic("--replicaof needs \"<host> <port>\"")
		}
		masterPort, err := strconv.Atoi(fields[1])
		if err != nil {
			panic("--replicaof port is not a number: " + fields[1])
		}

		store.info.Replication.Role = RoleSlave
		store.info.Replication.MasterHost = fields[0]
		store.info.Replication.MasterPort = masterPort
		go handleReplica(store.info.Replication.MasterHost, store.info.Replication.MasterPort, *port)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting client: %v\n", err)
			continue
		}
		go handleClient(conn, store)
	}
}
