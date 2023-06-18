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

const (
	TaskTypeMap    = 1
	TaskTypeReduce = 2
	TaskTypeExit   = 3
)

// Add your RPC definitions here.
type RequestArgs struct {
}

type RequestReply struct {
	TaskType int

	// return an id greater or equal than 0 if a mapper is needed, otherwise -1.
	MapperID  int
	ReducerID int
	ReduceNum int
	MapperNum int
	FilePath  string
}

type CompleteArgs struct {
}

type CompleteReply struct {
	TaskType int
}

type EmitIntermediateResultArgs struct {
	// id = -1 if no more result
	MapperID  int
	ReducerID int
}

type EmitIntermediateResultReply struct {
	Ack bool
}

type CompleteMapTaskArgs struct {
	MapperID int
}

type CompleteMapTaskReply struct {
	Ack bool
}

type ReceiveReduceTaskArgs struct {
	ReducerID int
}

type ReceiveReduceTaskReply struct {
	MapperID int
}

type CompleteReduceTaskArgs struct {
	ReducerID int
}

type CompleteReduceTaskReply struct {
	Ack bool
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
