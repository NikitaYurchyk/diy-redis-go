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

		if _, err := writer.WriteString(handler.Handle(ParseCommand(parts))); err != nil {
			fmt.Printf("Error writing to %s: %v\n", conn.RemoteAddr(), err)
			return
		}
		if err := writer.Flush(); err != nil {
			fmt.Printf("Error flushing to %s: %v\n", conn.RemoteAddr(), err)
			return
		}
	}
}

func startReplication(host string, port int) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Error connecting to master %s: %v\n", addr, err)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	if _, err := conn.Write([]byte(buildArray([]string{"PING"}))); err != nil {
		fmt.Printf("Error sending PING to master: %v\n", err)
		return
	}

	reply, err := readLine(reader)
	if err != nil {
		fmt.Printf("Error reading PING reply: %v\n", err)
		return
	}
	fmt.Printf("Master replied to PING: %s\n", reply)

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
		go startReplication(store.info.Replication.MasterHost, store.info.Replication.MasterPort)
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
