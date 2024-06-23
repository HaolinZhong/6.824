package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce    int
	nMapDone   int
	unassigned []*MapTask
	assigned   map[int]*MapTask
	files      []string
	mutex      sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
type MapTask struct {
	TaskId   int
	Filename string
	Status   int
}

const (
	TASK_UNASSIGNED = 0
	TASK_PENDING    = 1
	TASK_DONE       = 2

	MAX_PENDING_SECONDS = 10
)

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) StartMap(args *MapRequest, reply *MapResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.NReduce = c.nReduce

	if c.nMapDone >= len(c.files) {
		reply.Exception = MAP_EXCEPTION_ALL_TASK_COMPELETED
		return nil
	}

	if len(c.unassigned) == 0 {
		reply.Exception = MAP_EXCEPTION_ALL_TASK_ASSIGNED
		return nil
	}

	reply.Task = *c.unassigned[0]

	fmt.Printf("coordinator: map out  for task %d, file %s\n", reply.Task.TaskId, reply.Task.Filename)

	c.unassigned = c.unassigned[1:]
	c.assigned[reply.Task.TaskId] = &reply.Task
	reply.Task.Status = TASK_PENDING

	defer time.AfterFunc(MAX_PENDING_SECONDS*time.Second, func() {
		c.checkTaskStatus(reply.Task.TaskId)
	})

	return nil
}

func (c *Coordinator) DoneMap(args *MapDoneRequest, reply *MapDoneResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	task := c.assigned[args.TaskId]
	fmt.Printf("coordinator: map done for task %d, file %s\n", task.TaskId, task.Filename)
	task.Status = TASK_DONE
	c.nMapDone++

	return nil
}

func (c *Coordinator) checkTaskStatus(taskId int) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	task := c.assigned[taskId]
	if task.Status == TASK_PENDING {
		fmt.Printf("coordinator: map dead for task %d, file %s\n", task.TaskId, task.Filename)
		task.Status = TASK_UNASSIGNED
		delete(c.assigned, task.TaskId)
		c.unassigned = append(c.unassigned, task)
	}
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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:    nReduce,
		nMapDone:   0,
		unassigned: make([]*MapTask, 0),
		assigned:   make(map[int]*MapTask),
		files:      files,
		mutex:      sync.Mutex{},
	}

	// Your code here.
	for idx, filename := range files {
		task := MapTask{
			TaskId:   idx,
			Filename: filename,
			Status:   TASK_UNASSIGNED,
		}
		c.unassigned = append(c.unassigned, &task)
	}

	c.server()
	return &c
}
