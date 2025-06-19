package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "encoding/json"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	fmt.Println("Worker started...")

	// uncomment to send the Example RPC to the coordinator.
	// RequestExample()

	log.Printf("Worker: Asking for a task...")
	// Worker listens for tasks from the coordinator.
	for {
		// Asks the coordinator for a task.
		// Create the arguments for our RPC call. It's empty.
		args := RequestTaskArgs{}

		// Create a reply variable to hold the response from the coordinator.
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			fmt.Println("Worker: Coordinator is not available. Exiting.")
			return // Exit if the coordinator is not available.
		}

		// Check the type of task received.
		switch reply.TaskType {
			case "Map":
			// Handle Map task.
			fmt.Printf("Worker: Received Map task #%d for file %s\n", reply.TaskNumber, reply.FileName)
			performMapTask(reply.FileName, reply.TaskNumber, reply.NReduce, mapf)
			reportTaskDone("Map", reply.TaskNumber)
			case "Reduce":
				// We will implement this in a future step.
				fmt.Printf("Worker received Reduce task #%d\n", reply.TaskNumber)
			case "Wait":
				// The coordinator told us to wait.
				fmt.Println("No tasks available. Worker will wait and retry.")
				time.Sleep(1 * time.Second)
			case "Exit":
				// The coordinator told us the job is done.
				fmt.Println("Job is complete. Worker exiting.")
				return // Exit the Worker function, terminating the process.
			default:
				fmt.Printf("Received unknown task type: %s\n", reply.TaskType)
			}
 		}

 	}



// performMapTask executes a single Map task.
func performMapTask(filename string, taskNumber int, nReduce int, mapf func(string, string) []KeyValue) {
	// 1. Read the input file.
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Worker: Failed to open file %s: %v", filename, err)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Worker: Failed to read file %s: %v", filename, err)
	}
	file.Close()

	// 2. Call mapf to perform the map operation.
	//    THIS IS THE CRITICAL FIX!
	//    We pass the content of the file to the map function, which returns
	//    a slice of key-value pairs.
	kva := mapf(filename, string(content))

	// 3. Create nReduce temporary files and corresponding JSON encoders.
	//    We will write the intermediate key-value pairs to these files.
	encoders := make([]*json.Encoder, nReduce)
	tempFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		// Create a temporary file. It will be renamed later.
		tempFile, err := ioutil.TempFile("", "mr-map-temp-")
		if err != nil {
			log.Fatalf("Worker: Cannot create temporary file: %v", err)
		}
		tempFiles[i] = tempFile
		encoders[i] = json.NewEncoder(tempFile)
	}

	// 4. Distribute the key-value pairs from mapf into the nReduce buckets.
	for _, kv := range kva {
		// Use the ihash function to decide which bucket this key belongs to.
		reduceTaskNumber := ihash(kv.Key) % nReduce
		// Write the KeyValue pair to the correct temporary file.
		err := encoders[reduceTaskNumber].Encode(&kv)
		if err != nil {
			log.Fatalf("Worker: Cannot write to intermediate file: %v", err)
		}
	}

	// 5. Atomically rename the temporary files to their final intermediate names.
	//    This ensures that no other process sees a partially written file.
	for i := 0; i < nReduce; i++ {
		// First, close the file to make sure all data is written to disk.
		tempFiles[i].Close()
		tempName := tempFiles[i].Name()
		// The final name convention is mr-X-Y.
		finalName := fmt.Sprintf("mr-%d-%d", taskNumber, i)
		err := os.Rename(tempName, finalName)
		if err != nil {
			log.Fatalf("Worker: Cannot rename file %s to %s: %v", tempName, finalName, err)
		}
	}

	log.Printf("Worker: Finished Map task #%d.", taskNumber)
}

// Helper function to report that a task is done.
func reportTaskDone(taskType string, taskNumber int) {
	args := ReportTaskDoneArgs{
		TaskType:   taskType,
		TaskNumber: taskNumber,
	}
	reply := ReportTaskDoneReply{}
	ok := call("Coordinator.ReportTaskDone", &args, &reply)
	if !ok {
		fmt.Println("Failed to report task done. Coordinator may have exited.")
	}
}

func RequestExample() {

	// Create the arguments for our RPC call. It's empty.
	args := RequestTaskArgs{}

	// Create a reply variable to hold the response from the coordinator.
	reply := RequestTaskReply{}

	// Make the RPC call to the coordinator.
	// The first argument is the name of the RPC method we want to call.
	// It's formatted as "Coordinator.MethodName".
	ok := call("Coordinator.RequestTask", &args, &reply)

	// Check if the call was successful.
	if ok {
		// If the call was successful, the coordinator's reply is in our `reply` variable.
		// Let's print it out to see what we got!
		fmt.Printf("Worker received a reply: %+v\n", reply)
	} else {
		// If the call fails, it probably means the coordinator has exited.
		// We should probably exit too.
		fmt.Println("Call to coordinator failed. Worker exiting.")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
