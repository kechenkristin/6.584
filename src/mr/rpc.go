package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
// RequestTaskArgs is what the worker sends to ask for a task.
// It can be empty because the act of calling the function is the request.
type RequestTaskArgs struct {
}

type RequestTaskReply struct {
 TaskType string // "Map" or "Reduce"
 TaskNumber int // the task number, starting at 0
 FileName string // the name of the file to process (for Map tasks)
 NReduce int // the number of reduce tasks (for Map tasks)
 }

 // Add your RPC definitions here.

// ... (keep the other structs: RequestTaskArgs, RequestTaskReply)

// ReportTaskDoneArgs is what the worker sends to report a completed task.
type ReportTaskDoneArgs struct {
	TaskType   string
	TaskNumber int
}

// ReportTaskDoneReply is the coordinator's empty reply.
type ReportTaskDoneReply struct {
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
