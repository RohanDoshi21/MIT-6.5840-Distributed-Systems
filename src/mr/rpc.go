package mr

import (
	"os"
	"strconv"
	"time"
)

//
// Data Structures
//

type Task int

const (
	Exit Task = iota
	Wait
	Map
	Reduce
)

type Status int

const (
	Unassigned Status = iota
	Assigned
	Finished
)

type MapReduceTask struct {
	Task      Task
	Status    Status
	TimeStamp time.Time
	Index     int

	InputFiles  []string
	OutputFiles []string
}

//
// RPC definitions.
//
// remember to capitalize all names.
//

type RequestTaskReply struct {
	TaskNo  int
	Task    MapReduceTask
	NReduce int
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
