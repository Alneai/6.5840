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
	mapLength        int
	files            []string
	nReduce          int
	doneMap          []bool
	doneReduce       []bool
	lastAssignMap    []int64
	lastAssignReduce []int64
	isMapDone        bool
	isReduceDone     bool
}

var lock sync.Mutex

func isTaskAvailable(last int64) bool {
	return last == 0 || time.Now().Unix()-last > 10
}

// 目前实现是所有rpc串行的
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	lock.Lock()
	defer lock.Unlock()

	if c.isReduceDone {
		reply.Done = true
	} else if c.isMapDone {
		for i := 0; i < c.nReduce; i++ {
			if !c.doneReduce[i] && isTaskAvailable(c.lastAssignReduce[i]) {
				c.lastAssignReduce[i] = time.Now().Unix()
				reply.Filename = ""
				reply.TaskType = 2
				reply.TaskId = i
				reply.NReduce = c.nReduce
				log.Printf("assign reduce task %d\n", i)
				break
			}
		}
	} else {
		for i := 0; i < c.mapLength; i++ {
			if !c.doneMap[i] && isTaskAvailable(c.lastAssignMap[i]) {
				c.lastAssignMap[i] = time.Now().Unix()
				reply.Filename = c.files[i]
				reply.TaskType = 1
				reply.TaskId = i
				reply.NReduce = c.nReduce
				log.Printf("assign map task %d\n", i)
				break
			}
		}
	}
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	lock.Lock()
	defer lock.Unlock()

	if args.TaskType == 1 {
		c.doneMap[args.TaskId] = true
		log.Printf("map task %v is done\n", args.TaskId)
		done := true
		for _, v := range c.doneMap {
			if !v {
				done = false
				break
			}
		}
		if done {
			c.isMapDone = true
			log.Println("all map is done!")
		}
	} else {
		c.doneReduce[args.TaskId] = true
		log.Printf("reduce task %v is done\n", args.TaskId)
		done := true
		for _, v := range c.doneReduce {
			if !v {
				done = false
				break
			}
		}
		if done {
			c.isReduceDone = true
			log.Println("all reduce is done!")
		}
	}
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
	ret := c.isReduceDone
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapLength = len(files)
	c.files = files
	c.nReduce = nReduce
	c.doneMap = make([]bool, c.mapLength)
	c.doneReduce = make([]bool, nReduce)
	c.lastAssignMap = make([]int64, c.mapLength)
	c.lastAssignReduce = make([]int64, nReduce)

	c.server()
	return &c
}
