package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.

	files         []string
	intermediates []string
	nReduce       int
	fileWorker    map[int]string
	lastGivenID   int
	stage         string
}

var mutex sync.Mutex
var group2 sync.WaitGroup

var t = 10

func (c *Coordinator) HandleWorker(args *Args, reply *Reply) error {

	switch cmd := args.Command; cmd {

	case "Give":
		c.cmdGive(reply)

	case "Done Mapping":
		c.cmdMapDone(args)

	case "Done":
		c.cmdDone(args, reply)

	default:
		break

	}

	return nil

}

func (c *Coordinator) cmdGive(reply *Reply) {
	mutex.Lock()
	stage := c.stage
	mutex.Unlock()

	if stage == "Map" {

		mutex.Lock()
		if len(c.files) == 0 {
			reply.Command = "Sleep"
			mutex.Unlock()
			return
		}
		workerID := c.lastGivenID
		c.lastGivenID = workerID + 1
		filename := c.files[0]
		c.files = c.files[1:]
		reply.Command = c.stage

		c.fileWorker[workerID] = filename
		mutex.Unlock()

		reply.WorkerID = workerID
		reply.NReduce = c.nReduce
		reply.Content = filename

		//go c.asyncCheck(t, workerID, "Map")

	} else if stage == "Reduce" {

		mutex.Lock()

		if len(c.intermediates) == 0 {
			reply.Command = "Sleep"
			mutex.Unlock()
			return
		}

		filename := c.intermediates[0]
		c.intermediates = c.intermediates[1:]
		workerID := c.lastGivenID
		c.lastGivenID = workerID + 1
		reply.Command = c.stage

		mutex.Unlock()

		reply.WorkerID = workerID
		reply.NReduce = c.nReduce

		reply.Content = filename

		mutex.Lock()
		c.fileWorker[workerID] = filename
		mutex.Unlock()

		//go c.asyncCheck(t, workerID, "Reduce")

	}

}

func (c *Coordinator) cmdMapDone(args *Args) {
	mutex.Lock()

	if len(c.intermediates) == 0 {
		for i := 0; i < c.nReduce; i++ {
			filename := "mr-out-*-" + strconv.Itoa(i)
			//fmt.Println(filename)
			c.intermediates = append(c.intermediates, filename)
		}

	}

	delete(c.fileWorker, args.WorkerID)
	if len(c.files) == 0 && len(c.fileWorker) == 0 {
		c.lastGivenID = 0
		c.stage = "Reduce"
		//reply.Command = "Well done"
	}
	mutex.Unlock()

}

func (c *Coordinator) cmdDone(args *Args, reply *Reply) {
	mutex.Lock()

	if _, ok := c.fileWorker[args.WorkerID]; !ok {
		reply.Command = "TOO LATE YOU SLOW POS"
		mutex.Unlock()
		return
	}
	delete(c.fileWorker, args.WorkerID)
	if len(c.intermediates) == 0 && len(c.fileWorker) == 0 {
		c.lastGivenID = 0
		c.stage = "Done"
	}
	mutex.Unlock()
	reply.Command = "Well done"
}

func (c *Coordinator) asyncCheck(sleepSeconds int, workerID int, stage string) {
	time.Sleep(time.Duration(sleepSeconds) * time.Second)
	mutex.Lock()
	defer mutex.Unlock()

	switch coordStage := c.stage; coordStage {

	case "Map":
		if stage != "Map" {
			break
		}
		if file, ok := c.fileWorker[workerID]; ok {
			c.files = append(c.files, file)
			delete(c.fileWorker, workerID)
		}

	case "Reduce":
		if stage != "Reduce" {
			break
		}
		if file, ok := c.fileWorker[workerID]; ok {
			c.intermediates = append(c.intermediates, file)
			delete(c.fileWorker, workerID)
		}

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
	mutex.Lock()
	if c.stage == "Done" {
		ret = true
	}
	mutex.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files, []string{}, nReduce, make(map[int]string), 0, "Map"}

	c.server()
	return &c
}
