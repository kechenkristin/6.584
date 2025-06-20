package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"


// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// ByKey defines a type for sorting a slice of KeyValue structs by their key.
type ByKey []KeyValue

// Len, Swap, and Less are the three methods needed to implement sort.Interface.
// By implementing this interface, we can use Go's generic sort.Sort function.

// Len returns the number of items in the slice.
func (a ByKey) Len() int           { return len(a) }
// Swap swaps the items at positions i and j.
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
// Less returns true if the item at i should come before the item at j.
// We are sorting alphabetically by key.
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
				log.Printf("Worker: Received Map task #%d for file %s\n", reply.TaskNumber, reply.FileName)
				performMapTask(reply.FileName, reply.TaskNumber, reply.NReduce, mapf)
				reportTaskDone("Map", reply.TaskNumber)
			case "Reduce":
				// We will implement this in a future step.
				log.Printf("Worker received Reduce task #%d\n", reply.TaskNumber)
				performReduceTask(reply.TaskNumber, reply.NMap, reducef)
				reportTaskDone("Reduce", reply.TaskNumber)				
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

func performReduceTask(reduceTaskNumber int, numMapTasks int, reducef func(string, []string) string) {
	// This will be inside our performReduceTask function

	// We need to know how many Map tasks there were to know which files to read.
	// The Coordinator should provide this. Let's assume we get it.
	// (For this lab, the number of map tasks is the number of input files.)
	intermediate := []KeyValue{}

	// Loop from map task 0 to the last map task.
	for i := 0; i < numMapTasks; i++ {
		// Construct the intermediate filename for this map task and our reduce task.
		filename := fmt.Sprintf("mr-%d-%d", i, reduceTaskNumber)

		// Open the file.
		file, err := os.Open(filename)
		if err != nil {
			// It's possible a map task produced no keys for this reduce task,
			// so the file might not exist. We can safely ignore this error.
			continue
		}

		// Use a JSON decoder to read the KeyValue pairs from the file.
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break // We've reached the end of the file.
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// This is the next part of our performReduceTask function

	// Sort the intermediate data by key.
	sort.Sort(ByKey(intermediate))

	// We will write our final output to a temporary file first.
	tempFile, _ := ioutil.TempFile("", "mr-reduce-temp-")

	// This loop is the core of the reduce processing.
	// It iterates over the sorted intermediate data, one group of identical keys at a time.
	i := 0
	for i < len(intermediate) {
		// Find the end of the current group of keys.
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		// We've found a group! The key is intermediate[i].Key.
		// The values are in intermediate[i] through intermediate[j-1].
		// Collect all the values for this key into a slice of strings.
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// Call the user's Reduce function to get the final result.
		output := reducef(intermediate[i].Key, values)

		// Write the result to our temporary output file.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		// Move to the beginning of the next group.
		i = j
	}

	// This is the very end of our performReduceTask function

	// Close the temporary file to ensure all data is written to disk.
	tempFile.Close()

	// The final output file name.
	finalName := fmt.Sprintf("mr-out-%d", reduceTaskNumber)

	// Atomically rename the temporary file to its final name.
	os.Rename(tempFile.Name(), finalName)
	log.Printf("Worker: Finished Reduce task #%d.", reduceTaskNumber)

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
