package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	StateUnReceived = -1
	StateUnAssigned = 0
	StateAssigined  = 1
	StateInProgress = 2
	StateComplete   = 4
)

type BaseTask struct {
	State int
	Lock  sync.Mutex
}

type MapTask struct {
	BaseTask
	MapID    int
	FilePath string
}

type IntermediateResult struct {
	State     int
	MapID     int
	ReducerID int
}

type ReduceTask struct {
	BaseTask
	ReducerID              int
	IntermediateResultChan chan *IntermediateResult
}

func NewMapTask(id int, filePath string) MapTask {
	return MapTask{
		BaseTask: BaseTask{
			State: StateUnAssigned,
		},
		MapID:    id,
		FilePath: filePath,
	}
}

func NewReduceTask(id int, bufLen int) ReduceTask {
	return ReduceTask{
		BaseTask: BaseTask{
			State: StateUnAssigned,
		},
		ReducerID:              id,
		IntermediateResultChan: make(chan *IntermediateResult, bufLen),
	}
}

type Coordinator struct {
	MapTaskBufChan         chan *MapTask
	ReduceTaskBufChan      chan *ReduceTask
	MapTask                []MapTask
	IntermediateResultLock sync.Mutex
	IntermediateResult     []IntermediateResult
	ReduceTaskLock         sync.Mutex
	ReduceTask             []ReduceTask
	ReducerNum             int
	MapperNum              int
	ArgFiles               []string
}

func (c *Coordinator) Request(args *RequestArgs, reply *RequestReply) error {
	for !c.Done() {
		select {
		case mapTask := <-c.MapTaskBufChan:
			mapTask.Lock.Lock()
			defer mapTask.Lock.Unlock()
			mapTask.State = StateAssigined
			reply.TaskType = TaskTypeMap
			reply.ReduceNum = c.ReducerNum
			reply.MapperID = mapTask.MapID
			reply.FilePath = mapTask.FilePath
			go func(tsk *MapTask) {
				time.Sleep(10 * time.Second)
				// check if the task completes after 10 seconds. if not, reassigned it to another worker.
				tsk.Lock.Lock()
				defer tsk.Lock.Unlock()
				if tsk.State != StateComplete {
					fmt.Printf("[Coordinator] Map %d rescheduled.\n", tsk.MapID)
					c.MapTaskBufChan <- tsk
				}
			}(mapTask)
			return nil
		default:
			// do nothing
			fmt.Println("no map task")
		}

		select {
		case reduceTask := <-c.ReduceTaskBufChan:
			reduceTask.Lock.Lock()
			defer reduceTask.Lock.Unlock()
			reduceTask.State = StateAssigined
			reply.TaskType = TaskTypeReduce
			reply.MapperNum = c.MapperNum
			reply.ReducerID = reduceTask.ReducerID
			go func(tsk *ReduceTask) {
				time.Sleep(10 * time.Second)
				// check if the task completes after 10 seconds. if not, reassigned it to another worker.
				tsk.Lock.Lock()
				defer tsk.Lock.Unlock()
				if tsk.State != StateComplete {
					fmt.Printf("[Coordinator] Reducer %d rescheduled.\n", tsk.ReducerID)
					c.ReduceTaskBufChan <- tsk
					// reassign the tasks that are previously received
					for i := tsk.ReducerID; i < len(c.IntermediateResult); i += c.ReducerNum {
						if c.IntermediateResult[i].State == StateAssigined {
							tsk.IntermediateResultChan <- &c.IntermediateResult[i]
						}
					}
				}
			}(reduceTask)
			return nil
		default:
			fmt.Println("no reduce task")
		}

		fmt.Println("no available task. sleep for 1 second.")
		// if no available task, sleep for 100 mseconds
		time.Sleep(1 * time.Millisecond)
	}

	reply.TaskType = TaskTypeExit
	return nil
}

func (c *Coordinator) EmitIntermediateResult(args *EmitIntermediateResultArgs, reply *EmitIntermediateResultReply) error {
	mapID := args.MapperID
	reducerID := args.ReducerID
	index := c.ReducerNum*mapID + reducerID

	c.IntermediateResult[index] = IntermediateResult{
		MapID:     mapID,
		ReducerID: reducerID,
		State:     StateUnAssigned,
	}

	c.ReduceTask[reducerID].IntermediateResultChan <- &c.IntermediateResult[index]

	reply.Ack = true
	return nil
}

func (c *Coordinator) CompleteMapTask(args *CompleteMapTaskArgs, reply *CompleteMapTaskReply) error {
	mapID := args.MapperID
	c.MapTask[mapID].Lock.Lock()
	defer c.MapTask[mapID].Lock.Unlock()
	c.MapTask[mapID].State = StateComplete
	return nil
}

func (c *Coordinator) ReceiveReduceTask(args *ReceiveReduceTaskArgs, reply *ReceiveReduceTaskReply) error {
	reducerID := args.ReducerID
	result := <-c.ReduceTask[reducerID].IntermediateResultChan
	result.State = StateAssigined
	reply.MapperID = result.MapID
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *CompleteReduceTaskArgs, reply *CompleteReduceTaskReply) error {
	reducerID := args.ReducerID
	c.ReduceTask[reducerID].Lock.Lock()
	defer c.ReduceTask[reducerID].Lock.Unlock()
	c.ReduceTask[reducerID].State = StateComplete
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
	// Your code here.
	for i := range c.ReduceTask {
		if c.ReduceTask[i].State != StateComplete {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTaskBufChan:     make(chan *MapTask, len(files)),
		ReduceTaskBufChan:  make(chan *ReduceTask, nReduce),
		ReduceTask:         make([]ReduceTask, nReduce),
		IntermediateResult: make([]IntermediateResult, len(files)*nReduce),
		ReducerNum:         nReduce,
		MapperNum:          len(files),
		MapTask:            make([]MapTask, len(files)),
		ArgFiles:           files,
	}

	// Your code here.
	for i, file := range files {
		c.MapTask[i] = NewMapTask(i, file)
		c.MapTaskBufChan <- &c.MapTask[i]
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTask[i] = NewReduceTask(i, len(files))
		c.ReduceTaskBufChan <- &c.ReduceTask[i]
	}

	c.server()
	return &c
}
