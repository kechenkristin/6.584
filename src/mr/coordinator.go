package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

// Define the possible states for a task.
type TaskStatus int
const (
	Todo TaskStatus = iota // Value is 0
	InProgress             // Value is 1
	Done                   // Value is 2
)

// Define a structure to hold all information about a task.
type Task struct {
	// The unique number for this task (e.g., Map Task #3)
	TaskNumber int

	// The current status of this task (Todo, InProgress, or Done).
	Status     TaskStatus

	// The time this task was handed out to a worker.
	// This is for the 10-second crash detection rule.
	StartTime  time.Time

	// The input file for this task.
	// NOTE: This will only be used for Map tasks.
	// For Reduce tasks, this field will be empty.
	FileName   string
}


type Coordinator struct {
	// The new "To-Do" lists for Map and Reduce jobs.
	MapTasks    []Task
	ReduceTasks []Task

	// The big arrow on the wall showing the project's current phase.
	Phase       string

	// The lock on the office door.
	mu          sync.Mutex

	// We still need to remember this number.
	NReduce     int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// For now, let's keep it simple. We'll just give out the first Map task.
	// In the real version, you'll have logic here to find an available task.

	// A worker has knocked. First things first: lock the office door.
	c.mu.Lock()
	// And promise to unlock it when this conversation is done.
	defer c.mu.Unlock()

	switch c.Phase {
		case "Map":
			for i := range c.MapTasks {
				if c.MapTasks[i].Status == Todo {
					// YES! We found an available job. The Boss now does a few things.
	
					// 1. Update the whiteboard: Erase "To-Do" and write "In Progress".
					c.MapTasks[i].Status = InProgress
					// 2. Look at the clock and note the start time (for the 10-second crash rule).
					c.MapTasks[i].StartTime = time.Now()
	
					// 3. Fill out the "work order" (the 'reply') for the Worker.
					reply.TaskType = "Map"
					reply.TaskNumber = c.MapTasks[i].TaskNumber
					reply.NReduce = c.NReduce
					reply.FileName = c.MapTasks[i].FileName
	
					// 4. The conversation is over. The Boss hands the work order to the worker
					//    and sends them on their way.
					return nil
				}
			}
			reply.TaskType = "Wait"
		case "Reduce":
			for i := range c.ReduceTasks {
				if c.ReduceTasks[i].Status == Todo {
					// YES! We found an available job. The Boss now does a few things.
	
					// 1. Update the whiteboard: Erase "To-Do" and write "In Progress".
					c.ReduceTasks[i].Status = InProgress
					// 2. Look at the clock and note the start time (for the 10-second crash rule).
					c.ReduceTasks[i].StartTime = time.Now()
	
					// 3. Fill out the "work order" (the 'reply') for the Worker.
					reply.TaskType = "Reduce"
					reply.TaskNumber = c.ReduceTasks[i].TaskNumber
	
					// 4. The conversation is over. The Boss hands the work order to the worker
					//    and sends them on their way.
					reply.NReduce = c.NReduce // Number of Reduce tasks
					reply.NMap = len(c.MapTasks) // Number of Map tasks for Reduce workers
					return nil
				}
			}
			reply.TaskType = "Wait"
		case "Finished":
			// The Boss tells the worker: "Great news, the job is totally done!"
			reply.TaskType = "Exit"
		}

		log.Printf("Coordinator: Assigning %s task #%d to a worker.", reply.TaskType, reply.TaskNumber)
	
		// The conversation ends successfully.
		return nil
	}

	func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
		// The worker is back. Lock the door so we can update the whiteboard safely.
		c.mu.Lock()
		defer c.mu.Unlock()

		log.Printf("Coordinator: Worker has completed %s task #%d.", args.TaskType, args.TaskNumber)
	
		// The worker tells the Boss which task they finished ('args').
		// The Boss finds that task on the whiteboard...
		if args.TaskType == "Map" {
			// ...and updates its status from "In Progress" to "Done".
			c.MapTasks[args.TaskNumber].Status = Done
		} else if args.TaskType == "Reduce" {
			c.ReduceTasks[args.TaskNumber].Status = Done
		}
	
		// THIS IS THE MOST IMPORTANT PART OF THIS CONVERSATION!
		// After marking a task as done, the Boss must check if the whole phase is complete.
		if c.Phase == "Map" {
			// Assume for a second that all Map tasks are done.
			allMapDone := true
			// Now, double-check by looking at every single Map task on the board.
			for _, task := range c.MapTasks {
				// If we find even ONE task that is NOT marked "Done"...
				if task.Status != Done {
					// ...then our assumption was wrong. The phase is not over.
					allMapDone = false
					break // Stop checking, we have our answer.
				}
			}
	
			// If, after checking all of them, our assumption is still true...
			if allMapDone {
				// ...the Boss shouts "Hooray! All sorting is done! Time to start counting!"
				// and moves the project marker to the next phase.
				log.Println("All Map tasks complete. Transitioning to Reduce phase.")
				c.Phase = "Reduce"
			}
		} else if c.Phase == "Reduce" {
			// The logic is identical for the Reduce phase. Check if all Reduce tasks are done.
			allReduceDone := true
			for _, task := range c.ReduceTasks {
				if task.Status != Done {
					allReduceDone = false
					break
				}
			}
			if allReduceDone {
				// If they are, the entire project is finished!
				log.Println("All Reduce tasks complete. Job is finished.")
				c.Phase = "Finished"
			}
		}
	
		// The conversation ends.
		return nil
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
	c.mu.Lock()
	defer c.mu.Unlock()

	// If the phase is "Finished", then the job is done.
	if c.Phase == "Finished" {
		log.Println("Coordinator: Job is done.")
		return true
	}

	// Otherwise, the job is still in progress.
	log.Println("Coordinator: Job is still in progress.")
	return false
}


// Function to check for timed-out tasks in the background.
func (c *Coordinator) checkTimeouts() {
	for {
		time.Sleep(5 * time.Second) // Check every 5 seconds
		c.mu.Lock()
		if c.Phase == "Finished" {
			c.mu.Unlock()
			return
		}
		
		var tasks *[]Task
		if c.Phase == "Map" {
			tasks = &c.MapTasks
		} else {
			tasks = &c.ReduceTasks
		}

		for i := range *tasks {
			// Check if a task is in progress for more than 10 seconds
			if (*tasks)[i].Status == InProgress && time.Since((*tasks)[i].StartTime) > 10*time.Second {
				log.Printf("Task %d timed out. Resetting to Todo.", (*tasks)[i].TaskNumber)
				(*tasks)[i].Status = Todo
			}
		}
		c.mu.Unlock()
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks:    make([]Task, len(files)),
		ReduceTasks: make([]Task, nReduce),
		NReduce:     nReduce,
		Phase:       "Map",
	}

	// Initialize Map tasks
	for i, file := range files {
		c.MapTasks[i] = Task{TaskNumber: i, Status: Todo, FileName: file}
	}

	// Initialize Reduce tasks
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{TaskNumber: i, Status: Todo}
	}

	log.Println("Coordinator has been created and initialized.")
	
	// Start the background timeout checker
	go c.checkTimeouts()
	
	c.server()
	return &c
}