package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := MapRequest{}
		response := MapResponse{}
		call("Coordinator.StartMap", &args, &response)

		if response.Exception == 0 {

			filename := response.Task.Filename
			taskId := response.Task.TaskId
			nReduce := response.NReduce

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			// 调用word count程序提供的mapfunc来进行计算, 得到kv pair的slice
			kva := mapf(filename, string(content))

			if err := writeKVpairToFile(kva, taskId, nReduce); err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("map done for task %d, file %s\n", taskId, filename)
			call("Coordinator.DoneMap", &MapDoneRequest{TaskId: taskId}, &MapDoneResponse{})

		} else if response.Exception == MAP_EXCEPTION_ALL_TASK_ASSIGNED {
			fmt.Printf("worker: all task were assigned %s\n", time.Now())
			time.Sleep(time.Second)

		} else if response.Exception == MAP_EXCEPTION_ALL_TASK_COMPELETED {
			fmt.Printf("worker: all task were completed\n")
			break
		}
	}

	//call("Coordinator.StartReduce", &ReduceRequest{}, &ReduceResponse{})
	fmt.Println("enter reduce")

}

func writeKVpairToFile(kva []KeyValue, taskId, nReduce int) error {
	idxToFileMap := make(map[int]*os.File)

	for i := 0; i < nReduce; i++ {
		tmpFile, err := ioutil.TempFile("", fmt.Sprintf("temp-%d-%d.json", taskId, i))
		if err != nil {
			return errors.New("failed to create temp file")
		}
		defer tmpFile.Close()
		idxToFileMap[i] = tmpFile
	}

	for _, kv := range kva {
		jsonData, err := json.Marshal(kv)
		if err != nil {
			return errors.New("failed to encode json data")
		}

		file := idxToFileMap[ihash(kv.Key)%nReduce]

		if _, err := file.Write(jsonData); err != nil {
			fmt.Println(err)
			fmt.Printf("failed to write into file %s", file.Name())
			return errors.New("failed to write json data to file")
		}
	}

	for i := 0; i < nReduce; i++ {
		tmpFile := idxToFileMap[i]
		tmpFileName := tmpFile.Name()
		tmpFile.Close()

		if err := os.Rename(tmpFileName, fmt.Sprintf("mr-%d-%d.json", taskId, i)); err != nil {
			return errors.New("failed to rename temp file")
		}
	}

	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
