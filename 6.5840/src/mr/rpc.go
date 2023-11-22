package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskArgs struct {
	maptasknum    int // map task number only
	reducetasknum int // reduce task number only
}

type TaskReply struct {
	TaskType string // map or reduce or waiting
	FileName string
	NReduce  int // number of reduce tasks
	nMap     int // number of map tasks

	maptasknum    int // map task number only
	reducetasknum int // reduce task number only

}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
