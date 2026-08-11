package main

import (
	"fmt"
	"strings"
)

type RoleType string

const (
	RoleMaster RoleType = "master"
	RoleSlave  RoleType = "slave"
)

type Replication struct {
	Role                       RoleType
	ConnectedSlaves            uint64
	MasterReplID               string
	MasterReplOffset           uint64
	SecondReplOffset           int64
	ReplBacklogActive          uint64
	ReplBacklogSize            uint64
	ReplBacklogFirstByteOffset uint64
	ReplBacklogHistlen         uint64
	MasterHost string
	MasterPort int
}

func (r *Replication) RespReplication() string {
	lines := []string{
		"# Replication",
		fmt.Sprintf("role:%s", r.Role),
		fmt.Sprintf("connected_slaves:%d", r.ConnectedSlaves),
		fmt.Sprintf("master_replid:%s", r.MasterReplID),
		fmt.Sprintf("master_repl_offset:%d", r.MasterReplOffset),
		fmt.Sprintf("second_repl_offset:%d", r.SecondReplOffset),
		fmt.Sprintf("repl_backlog_active:%d", r.ReplBacklogActive),
		fmt.Sprintf("repl_backlog_size:%d", r.ReplBacklogSize),
		fmt.Sprintf("repl_backlog_first_byte_offset:%d", r.ReplBacklogFirstByteOffset),
		fmt.Sprintf("repl_backlog_histlen:%d", r.ReplBacklogHistlen),
	}

	return strings.Join(lines, crlf)
}

type Info struct {
	Replication Replication
}

func InitInfo() *Info {
	return &Info{
		Replication: Replication{
			Role:             RoleMaster,
			MasterReplID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			SecondReplOffset: -1,
			ReplBacklogSize:  1048576,
		},
	}
}
