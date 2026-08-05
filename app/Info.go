package main

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
