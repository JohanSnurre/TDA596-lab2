package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

// receive files and nreduce
// divide files into workers
// check when worker done and assign another file to it
// collect it in the end

// pick idle workers and assign each a task

type TaskInfo struct {
	fileIndex int
	status    int
}

type Coordinator struct {
	files       []string
	nReduce     int
	forMap      []TaskInfo  // index and done status
	forReduce   map[int]int // index for reduce and also mapped index?
	currentMapI int
}

// Your code here -- RPC handlers for the worker to call.

// master tasks:
// assign tasks
func (c *Coordinator) AssignTasks(args *Args, reply *Reply) error {
	if c.currentMapI < len(c.files) {
		// map
		currentFile := c.forMap[c.currentMapI]
		reply.File = c.files[currentFile.fileIndex]
		reply.IsMap = true
		reply.NReduce = c.nReduce
		reply.MapIndex = c.currentMapI
		c.currentMapI = c.currentMapI + 1

		fmt.Println("map")
		fmt.Println(reply.File)
		fmt.Println("###")
	} else {
		// reduce
		for i := 0; i < 1; i++ {
			reply.Intermediate = append(reply.Intermediate, "mr-X-"+strconv.Itoa(i))
		}
		reply.IsMap = false
		reply.NReduce = c.nReduce
		fmt.Println("reduce")
	}

	return nil
}

// collect mapped files
func (c *Coordinator) ManageMapTaskFinished(args *MapArgs, reply *Reply) error {
	fmt.Println("###################")
	fmt.Println(args.Intermediate)
	return nil
}

// collect reduced files

// 10 second thing check?

// check if worker finshd

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
	ret := len(c.forMap) == 0 && len(c.forReduce) == 0

	// Your code here.
	// traverse thorugh workers and see if they are finished

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nReduce = nReduce
	c.files = files
	c.forReduce = make(map[int]int, len(files))
	c.currentMapI = 0

	// assign tasks - file and done value
	for i := 0; i < len(files); i++ {
		taskInfo := TaskInfo{}
		taskInfo.fileIndex = i
		taskInfo.status = 0
		c.forMap = append(c.forMap, taskInfo)
	}

	c.server()
	return &c
}
