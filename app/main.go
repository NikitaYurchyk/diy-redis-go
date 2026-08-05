package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
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
		store.info.Replication.Role = RoleSlave
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
