package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce     int
	nMap        int
	mapTasks    []*Task
	reduceTasks []*Task
	lock        sync.Mutex
	stage       Stage
}
type Stage int

const (
	MapStage    Stage = 0
	ReduceStage Stage = 1
	AllDone     Stage = 2
)

type TaskState int

const (
	Idle        TaskState = 0
	In_Progress TaskState = 1
	Completed   TaskState = 2
)

type Task struct {
	Id        int
	Type      TaskType
	State     TaskState
	Filename  string
	StartTime time.Time
}

// Your code here -- RPC handlers for the worker to call.

// RPC handler for workers to ask a task and then assign a task
func (c *Coordinator) AssignTask(args *WorkerArgs, reply *TaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	switch c.stage {
	case MapStage:
		for _, task := range c.mapTasks {
			if task.State == Idle {
				// Task management
				task.State = In_Progress
				task.StartTime = time.Now()

				// Fill the reply
				reply.Type = Map
				reply.Filename = task.Filename
				reply.TaskID = task.Id
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap

				return nil
			}
		}

		// There is no idle map task, check whether all maptasks are completed
		for _, task := range c.mapTasks {
			if task.State != Completed {
				reply.Type = Wait
				return nil
			}
		}

		// All maptasks are done
		c.stage = ReduceStage
		fallthrough

	case ReduceStage:
		for _, task := range c.reduceTasks {
			if task.State == Idle {
				// Task management
				task.State = In_Progress
				task.StartTime = time.Now()

				// Fill the reply
				reply.Type = Reduce
				reply.TaskID = task.Id
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap

				return nil
			}
		}

		// There is no idle reduce task, check whether all reduce tasks are completed
		for _, task := range c.reduceTasks {
			if task.State != Completed {
				reply.Type = Wait
				return nil
			}
		}

		// All maptasks are done
		c.stage = AllDone
		reply.Type = Exit
		return nil
	default:
		// All task done
		reply.Type = Exit
		return nil
	}
}

// RPC handler for workers to report completetion
func (c *Coordinator) TaskDone(args *WorkerArgs, reply *TaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	switch args.Type {
	case Map:
		if c.mapTasks[args.TaskID].State == In_Progress {
			c.mapTasks[args.TaskID].State = Completed
		}
	case Reduce:
		if c.reduceTasks[args.TaskID].State == In_Progress {
			c.reduceTasks[args.TaskID].State = Completed
		}

		allDone := true
		for _, task := range c.reduceTasks {
			if task.State != Completed {
				allDone = false
				break
			}
		}
		if allDone {
			c.stage = AllDone
		}
	}
	return nil
}

// A background thread that check the overtime every second
func (c *Coordinator) ticker() {
	for {
		time.Sleep(1 * time.Second)
		c.lock.Lock()
		for _, task := range c.mapTasks {
			if task.State == In_Progress {
				if time.Since(task.StartTime) > 10*time.Second {
					task.State = Idle
				}
			}
		}

		for _, task := range c.reduceTasks {
			if task.State == In_Progress {
				if time.Since(task.StartTime) > 10*time.Second {
					task.State = Idle
				}
			}
		}
		c.lock.Unlock()
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.stage == AllDone {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.nMap = len(files)
	c.stage = MapStage
	// Initialize Map Tasks List
	c.mapTasks = make([]*Task, c.nMap)
	for i, file := range files {
		c.mapTasks[i] = &Task{
			Id:       i,
			Type:     Map,
			State:    Idle,
			Filename: file,
		}
	}

	// Initialize Reduce Tasks List
	c.reduceTasks = make([]*Task, c.nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &Task{
			Id:    i,
			Type:  Reduce,
			State: Idle,
		}
	}
	c.server()
	go c.ticker()
	return &c
}
