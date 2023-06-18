package mr

import (
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sync"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func intermediateFileName(m int, r int) string {
	return fmt.Sprintf("mr-intermediate-%d-%d", m, r)
}

func outputFileName(r int) string {
	return fmt.Sprintf("mr-out-%d", r)
}

func getNthKVs(kvs []KeyValue, divider int, hashKey int) []KeyValue {
	ret := make([]KeyValue, 0)
	for _, kv := range kvs {
		if ihash(kv.Key)%divider == hashKey {
			ret = append(ret, kv)
		}
	}
	return ret
}

func writeToCSV(kvs []KeyValue, fname string) (*os.File, error) {
	f, err := os.CreateTemp("", "mr-temp-*")
	defer f.Close()
	if err != nil {
		return nil, err
	}
	csvWriter := csv.NewWriter(f)
	for _, kv := range kvs {
		err = csvWriter.Write([]string{kv.Key, kv.Value})
		if err != nil {
			return nil, err
		}
	}
	csvWriter.Flush()
	os.Rename(f.Name(), fname)
	return f, nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply := Request()

		if reply.TaskType == TaskTypeMap {
			// read the corresponding file from the reply and generate kv
			// according to reducer's num
			// do something here
			f, err := os.Open(reply.FilePath)
			if err != nil {
				fmt.Printf("[Worker][Map] open file error: %s", err.Error())
			}
			content, err := io.ReadAll(f)
			if err != nil {
				fmt.Printf("[Worker][Map] read file error: %s", err.Error())
			}

			f.Close()

			kvs := mapf(reply.FilePath, string(content))

			// for each reduce task, generate an intermediate file and inform the coordinator
			// upon the completion of the file
			var wg sync.WaitGroup
			for i := 0; i < reply.ReduceNum; i++ {
				wg.Add(1)
				go func(reducerID int) {
					ithKVs := getNthKVs(kvs, reply.ReduceNum, reducerID)
					writeToCSV(ithKVs, intermediateFileName(reply.MapperID, reducerID))
					EmitIntermediateResult(reply.MapperID, reducerID)
					wg.Done()
				}(i)
			}
			wg.Wait()

			CompleteMapTask(reply.MapperID)
		} else if reply.TaskType == TaskTypeReduce {
			kCount := make(map[string][]string)
			var kCountLock sync.Mutex
			var wg sync.WaitGroup
			for i := 0; i < reply.MapperNum; i++ {
				wg.Add(1)
				go func() {
					reduceTaskReply := ReceiveReduceTask(reply.ReducerID)
					mapperID := reduceTaskReply.MapperID
					fname := intermediateFileName(mapperID, reply.ReducerID)
					f, err := os.Open(fname)
					if err != nil {
						fmt.Printf("[Worker][Reduce] open file error: %s\n", err.Error())
					}

					reader := csv.NewReader(f)
					records, err := reader.ReadAll()
					if err != nil {
						fmt.Printf("[Worker][Reduce] read file error %s: %s\n", fname, err.Error())
					}

					kCountLock.Lock()
					for _, record := range records {
						k := record[0]
						v := record[1]
						if _, exist := kCount[k]; !exist {
							kCount[k] = make([]string, 0)
						}
						kCount[k] = append(kCount[k], v)
					}
					kCountLock.Unlock()

					// remove intermediate file
					err = f.Close()
					if err != nil {
						fmt.Printf("[Worker][Reduce] close file error %s: %s\n", fname, err.Error())
					}
					wg.Done()
				}()
			}

			wg.Wait()

			outputFile, err := os.CreateTemp("", "mr-out-temp-*")
			if err != nil {
				fmt.Printf("[Worker][Reduce] create temp file error: %s\n", err.Error())
			}

			for k, v := range kCount {
				fmt.Fprintf(outputFile, "%s %s\n", k, reducef(k, v))
			}

			outputFile.Close()
			os.Rename(outputFile.Name(), outputFileName(reply.ReducerID))

			CompleteReduceTask(reply.ReducerID)
		} else {
			os.Exit(0)
		}
	}
}

func Request() *RequestReply {
	args := RequestArgs{}
	reply := RequestReply{}

	callname := "Coordinator.Request"

	ok := call(callname, &args, &reply)
	var message string
	if ok {
		message = "success"
	} else {
		message = "failed"
	}

	logRPCResult(callname, message, args, reply)

	return &reply
}

func logRPCResult(callname string, message string, args interface{}, reply interface{}) {
	fmt.Printf("[Worker] %s: %s\nargs - %+v\nreply - %+v\n", callname, message, args, reply)
}

func EmitIntermediateResult(mapID int, reduceID int) *EmitIntermediateResultReply {
	args := EmitIntermediateResultArgs{
		MapperID:  mapID,
		ReducerID: reduceID,
	}

	reply := EmitIntermediateResultReply{}

	callName := "Coordinator.EmitIntermediateResult"
	ok := call(callName, &args, &reply)
	var message string
	if ok {
		message = "success"
	} else {
		message = "failed"
	}

	logRPCResult(callName, message, args, reply)

	return &reply
}

func CompleteMapTask(mapID int) *CompleteMapTaskReply {
	args := CompleteMapTaskArgs{
		MapperID: mapID,
	}

	reply := CompleteMapTaskReply{}

	callName := "Coordinator.CompleteMapTask"
	ok := call(callName, &args, &reply)
	var message string
	if ok {
		message = "success"
	} else {
		message = "failed"
	}

	logRPCResult(callName, message, args, reply)

	return &reply
}

func ReceiveReduceTask(reducerID int) *ReceiveReduceTaskReply {
	args := ReceiveReduceTaskArgs{
		ReducerID: reducerID,
	}
	reply := ReceiveReduceTaskReply{}

	callName := "Coordinator.ReceiveReduceTask"
	ok := call(callName, &args, &reply)
	var message string
	if ok {
		message = "success"
	} else {
		message = "failed"
	}

	logRPCResult(callName, message, args, reply)

	return &reply
}

func CompleteReduceTask(reducerID int) *CompleteReduceTaskReply {
	args := CompleteReduceTaskArgs{ReducerID: reducerID}
	reply := CompleteReduceTaskReply{}

	callName := "Coordinator.CompleteReduceTask"
	ok := call(callName, &args, &reply)
	var message string
	if ok {
		message = "success"
	} else {
		message = "failed"
	}

	logRPCResult(callName, message, args, reply)

	return &reply
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
