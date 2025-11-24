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

// 1. Define a Enum to represent the tasktype.
type TaskType int

const (
	Map    TaskType = 0
	Reduce TaskType = 1
	Wait   TaskType = 2
	Exit   TaskType = 3
)

// 2. The Workers ask or report a task.
/*
	Map Task ID & Reduce Task ID
	TaskType
*/
type WorkerArgs struct {
	Type   TaskType
	TaskID int
}

// 3. The Coordinator assigns a task.
/*
	TaskType
	Filename: The input file's name
	Map Task ID & Reduce Task ID: mr-X-Y
	NReduce: The number of reduce tasks
	NMap: The number of map tasks
*/
type TaskReply struct {
	Type     TaskType
	Filename string
	TaskID   int
	NReduce  int
	NMap     int
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
