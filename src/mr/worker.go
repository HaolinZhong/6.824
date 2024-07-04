package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	//fmt.Println("worker started")

	MapPhase(mapf)

	//fmt.Println("enter reduce")

	ReducePhase(reducef)

	//fmt.Println("worker gracefully exit")

}

func MapPhase(mapf func(string, string) []KeyValue) {
	for {
		args := MapRequest{}
		response := MapResponse{}
		call("Coordinator.StartMap", &args, &response)

		if response.Exception == 0 {

			filename := response.Task.Filename
			taskId := response.Task.TaskId
			assignId := response.Task.AssignId
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
			//fmt.Printf("map done for task %d, file %s\n", taskId, filename)
			call("Coordinator.DoneMap", &MapDoneRequest{TaskId: taskId, AssignId: assignId}, &MapDoneResponse{})

		} else if response.Exception == EXCEPTION_ALL_TASK_ASSIGNED {
			//fmt.Printf("worker: all map task were assigned %s\n", time.Now())
			time.Sleep(time.Second)

		} else if response.Exception == EXCEPTION_ALL_TASK_COMPELETED {
			//fmt.Printf("worker: all map task were completed\n")
			break
		}
	}
}

func ReducePhase(reducef func(string, []string) string) {
	for {
		args := ReduceRequest{}
		response := ReduceResponse{}
		call("Coordinator.StartReduce", &args, &response)

		if response.Exception == 0 {

			taskId := response.Task.TaskId
			assignId := response.Task.AssignId
			pattern := fmt.Sprintf("mr-*-%d.json", taskId)
			intermediate := []KeyValue{}

			files, err := filepath.Glob(pattern)
			if err != nil {
				fmt.Printf("worker: failed to find files with pattern %s\n", pattern)
				continue
			}
			for _, filename := range files {
				kva, err := readKVpairFromFile(filename)
				//fmt.Print(kva)
				if err != nil {
					fmt.Printf("worker: failed to read KV from file %s\n", filename)
					continue
				}
				intermediate = append(intermediate, kva...)
			}

			sort.Sort(ByKey(intermediate))
			//fmt.Println(intermediate)

			toname := fmt.Sprintf("tmp-out-%d.out", taskId)
			oname := fmt.Sprintf("mr-out-%d.out", taskId)
			tmpFile, err := ioutil.TempFile("", toname)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				// 通过j来拿到slice里某个key所占据的范围
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				// 将专属于这个key的东西拿到values里面
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				// 把key和values给到reducer func, 执行并得到结果
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			tmpFile.Close()
			if err := os.Rename(tmpFile.Name(), oname); err != nil {
				fmt.Print(err)
				continue
			}

			//fmt.Printf("reduce done for task %d\n", taskId)
			call("Coordinator.DoneReduce", &ReduceDoneRequest{TaskId: taskId, AssignId: assignId}, &ReduceDoneResponse{})

		} else if response.Exception == EXCEPTION_ALL_TASK_ASSIGNED {
			//fmt.Printf("worker: all reduce task were assigned %s\n", time.Now())
			time.Sleep(1 * time.Second)

		} else if response.Exception == EXCEPTION_ALL_TASK_COMPELETED {
			//fmt.Printf("worker: all reduce task were completed\n")
			break
		}
	}
}

func writeKVpairToFile(kva []KeyValue, taskId, nReduce int) error {
	idxToFileMap := make(map[int]*os.File)
	idxToKvaMap := make(map[int][]KeyValue)

	for i := 0; i < nReduce; i++ {
		tmpFile, err := ioutil.TempFile("", fmt.Sprintf("temp-%d-%d.json", taskId, i))
		if err != nil {
			return errors.New("failed to create temp file")
		}
		defer tmpFile.Close()
		idxToFileMap[i] = tmpFile
		idxToKvaMap[i] = make([]KeyValue, 0)
	}

	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		kva := idxToKvaMap[idx]
		kva = append(kva, kv)
		idxToKvaMap[idx] = kva
	}

	for i := 0; i < nReduce; i++ {
		tmpFile := idxToFileMap[i]
		kva := idxToKvaMap[i]
		jsonData, err := json.Marshal(kva)
		if err != nil {
			return errors.New("failed to encode json data")
		}

		if _, err := tmpFile.Write(jsonData); err != nil {
			fmt.Println(err)
			fmt.Printf("failed to write into file %s", tmpFile.Name())
			return errors.New("failed to write json data to file")
		}

		tmpFile.Close()

		if err := os.Rename(tmpFile.Name(), fmt.Sprintf("mr-%d-%d.json", taskId, i)); err != nil {
			return errors.New("failed to rename temp file")
		}
	}

	return nil
}

func readKVpairFromFile(filename string) (kva []KeyValue, err error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	err = json.Unmarshal(content, &kva)
	if err != nil {
		fmt.Println(err)
		fmt.Printf("failed to unmarshal from file %s\n", filename)
		return kva, err
	}
	return kva, nil
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
