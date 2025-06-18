package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	InputFiles []string // list of input files
 	NReduce     int      // number of reduce tasks
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// For now, let's keep it simple. We'll just give out the first Map task.
	// In the real version, you'll have logic here to find an available task.

	// Let's log that a worker asked for a task.
	log.Println("Coordinator: Received a task request from a worker.")

	// Fill in the reply structure with the details of the first Map task.
	reply.TaskType = "Map"
	reply.TaskNumber = 1 // Let's say it's task #1
	reply.NReduce = c.NReduce
	reply.FileName = c.InputFiles[0] // Give it the first file

	log.Printf("Coordinator: Assigning Map task #%d with file %s\n", reply.TaskNumber, reply.FileName)

	return nil // A nil error means the RPC call was successful.	
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.InputFiles = files
 	c.NReduce = nReduce
	log.Printf("Coordinator: Created with %d input files and %d reduce tasks.\n", len(files), nReduce)

	c.server()
	return &c
}
