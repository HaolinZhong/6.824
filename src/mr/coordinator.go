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
	nReduce          int
	nMapDone         int
	nReduceDone      int
	unassignedMap    []*MapTask
	mapTasks         map[int]*MapTask
	unassignedReduce []*ReduceTask
	reduceTasks      map[int]*ReduceTask
	files            []string
	mutex            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
type MapTask struct {
	TaskId   int
	Filename string
	Status   int
	AssignId int
}

type ReduceTask struct {
	TaskId   int
	Status   int
	AssignId int
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
		reply.Exception = EXCEPTION_ALL_TASK_COMPELETED
		return nil
	}

	if len(c.unassignedMap) == 0 {
		reply.Exception = EXCEPTION_ALL_TASK_ASSIGNED
		return nil
	}

	// 不能直接把map里的task拿去作为返回值, 而是要复制一个
	// 否则, rpc的codec对map内task的访问是不被锁保护的, 会引发race
	task := c.unassignedMap[0]
	task.Status = TASK_PENDING
	task.AssignId++
	reply.Task = MapTask{
		TaskId:   task.TaskId,
		Filename: task.Filename,
		Status:   TASK_PENDING,
		AssignId: task.AssignId,
	}

	//fmt.Printf("coordinator: map out  for task %d, file %s\n", reply.Task.TaskId, reply.Task.Filename)
	c.unassignedMap = c.unassignedMap[1:]

	defer time.AfterFunc(MAX_PENDING_SECONDS*time.Second, func() {
		c.checkMapTaskStatus(task.TaskId)
	})

	return nil
}

func (c *Coordinator) DoneMap(args *MapDoneRequest, reply *MapDoneResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	task := c.mapTasks[args.TaskId]
	if task.Status == TASK_PENDING {
		//fmt.Printf("coordinator: map done for task %d, file %s\n", task.TaskId, task.Filename)
		task.Status = TASK_DONE
		c.nMapDone++
	} else {
		if task.Status != TASK_PENDING {
			fmt.Printf("coordinator: map done failed for task %d, file %s because task is no long pending\n", task.TaskId, task.Filename)
		}
	}

	return nil
}

func (c *Coordinator) checkMapTaskStatus(taskId int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task := c.mapTasks[taskId]
	if task != nil && task.Status == TASK_PENDING {
		fmt.Printf("coordinator: map dead for task %d, file %s\n", task.TaskId, task.Filename)
		task.Status = TASK_UNASSIGNED
		c.unassignedMap = append(c.unassignedMap, task)
	}
}

func (c *Coordinator) StartReduce(args *ReduceRequest, reply *ReduceResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.nReduceDone >= c.nReduce {
		reply.Exception = EXCEPTION_ALL_TASK_COMPELETED
		return nil
	}

	if len(c.unassignedReduce) == 0 {
		reply.Exception = EXCEPTION_ALL_TASK_ASSIGNED
		return nil
	}

	// 不能直接把map里的task拿去作为返回值, 而是要复制一个
	// 否则, rpc的codec对map内task的访问是不被锁保护的, 会引发race
	task := c.unassignedReduce[0]
	task.Status = TASK_PENDING
	task.AssignId++
	reply.Task = ReduceTask{
		TaskId:   task.TaskId,
		Status:   TASK_PENDING,
		AssignId: task.AssignId,
	}

	//fmt.Printf("coordinator: reduce out  for task %d\n", reply.Task.TaskId)

	c.unassignedReduce = c.unassignedReduce[1:]

	time.AfterFunc(MAX_PENDING_SECONDS*time.Second, func() {
		c.checkReduceTaskStatus(task.TaskId)
	})
	return nil
}

func (c *Coordinator) DoneReduce(args *ReduceDoneRequest, reply *ReduceDoneResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	task := c.reduceTasks[args.TaskId]
	if task.Status == TASK_PENDING {
		task.Status = TASK_DONE
		c.nReduceDone++
		//fmt.Printf("coordinator: reduce done for task %d\n", task.TaskId)
	}

	return nil
}

func (c *Coordinator) checkReduceTaskStatus(taskId int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	task := c.reduceTasks[taskId]
	if task != nil && task.Status == TASK_PENDING {
		fmt.Printf("coordinator: reduce dead for task %d\n", task.TaskId)
		task.Status = TASK_UNASSIGNED
		c.unassignedReduce = append(c.unassignedReduce, task)
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
	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.nReduceDone >= c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:          nReduce,
		nMapDone:         0,
		unassignedMap:    make([]*MapTask, 0),
		mapTasks:         make(map[int]*MapTask),
		unassignedReduce: make([]*ReduceTask, 0),
		reduceTasks:      make(map[int]*ReduceTask),
		files:            files,
		mutex:            sync.Mutex{},
	}

	// Your code here.

	// init map task
	for idx, filename := range files {
		task := MapTask{
			TaskId:   idx,
			Filename: filename,
			Status:   TASK_UNASSIGNED,
			AssignId: 0,
		}
		c.unassignedMap = append(c.unassignedMap, &task)
		c.mapTasks[idx] = &task
	}

	for i := 0; i < nReduce; i++ {
		task := ReduceTask{
			TaskId:   i,
			Status:   TASK_UNASSIGNED,
			AssignId: 0,
		}
		c.unassignedReduce = append(c.unassignedReduce, &task)
		c.reduceTasks[i] = &task
	}

	c.server()
	return &c
}
