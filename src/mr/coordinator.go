package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// var mapFiles chan Task

type Task struct {
	index  int
	status int
}

type Coordinator struct {
	// Your definitions here.

	files         []string
	mapFiles      map[int]int //0, 1, 2
	reduceFiles   map[int]int //0, 1, 2
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

func (c *Coordinator) getTask(tasks map[int]int) int {
	index := -1

	for i := range tasks {
		if tasks[i] == 0 {
			return i
		}
	}

	return index
}

func (c *Coordinator) cmdGive(reply *Reply) {
	mutex.Lock()
	stage := c.stage
	task := c.getTask(c.mapFiles)
	mutex.Unlock()

	if stage == "Map" {
		fmt.Println("map")

		if task == -1 {
			reply.Command = "Sleep"
			return
		}

		mutex.Lock()
		//fmt.Println("starting " + strconv.Itoa(task) + ": " + c.files[task])
		workerID := task
		filename := c.files[task]
		c.mapFiles[task] = 2
		// c.lastGivenID = workerID + 1
		// filename := c.files[0]
		// c.files = c.files[1:]
		reply.Command = c.stage

		c.fileWorker[workerID] = filename
		mutex.Unlock()

		reply.WorkerID = workerID
		reply.NReduce = c.nReduce
		reply.Content = filename

		//go c.asyncCheck(t, workerID, "Map")

		go c.mapAsyncCheck(workerID)

	} else if stage == "Reduce" {
		fmt.Println("REDUS")

		mutex.Lock()
		task = c.getTask(c.reduceFiles)

		if task == -1 {
			reply.Command = "Sleep"
			mutex.Unlock()
			return
		}

		// filename := c.intermediates[0]
		// c.intermediates = c.intermediates[1:]
		// workerID := c.lastGivenID
		// c.lastGivenID = workerID + 1
		workerID := task
		filename := c.intermediates[task]
		c.reduceFiles[task] = 2
		reply.Command = c.stage

		mutex.Unlock()

		reply.WorkerID = workerID
		reply.NReduce = c.nReduce

		reply.Content = filename

		mutex.Lock()
		c.fileWorker[workerID] = filename
		mutex.Unlock()

		// go c.asyncCheck(t, workerID, "Reduce")
		go c.reduceAsyncCheck(workerID)

	}

}

func (c *Coordinator) cmdMapDone(args *Args) {
	mutex.Lock()

	if len(c.intermediates) == 0 {
		for i := 0; i < c.nReduce; i++ {
			filename := "mr-o-*-" + strconv.Itoa(i)
			//fmt.Println(filename)
			c.intermediates = append(c.intermediates, filename)
		}

	}

	c.mapFiles[args.WorkerID] = 1

	mapDone := true
	for i := range c.mapFiles {
		if c.mapFiles[i] != 1 {
			mapDone = false
		}
	}

	if mapDone {
		c.stage = "Reduce"
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

	c.reduceFiles[args.WorkerID] = 1

	reduceDone := true
	for i := range c.reduceFiles {
		if c.reduceFiles[i] != 1 {
			reduceDone = false
		}
	}

	if reduceDone {
		c.stage = "Done"
	}

	mutex.Unlock()
	reply.Command = "Well done"
}

func (c *Coordinator) mapAsyncCheck(worker int) {
	time.Sleep(time.Duration(10) * time.Second)
	mutex.Lock()
	defer mutex.Unlock()

	if c.mapFiles[worker] != 1 { // worker not done - try again
		c.mapFiles[worker] = 0
	}
}

func (c *Coordinator) reduceAsyncCheck(worker int) {
	time.Sleep(time.Duration(10) * time.Second)
	mutex.Lock()
	defer mutex.Unlock()

	if c.reduceFiles[worker] != 1 { // worker not done - try again
		c.reduceFiles[worker] = 0
	}

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

func trackFiles(n int) map[int]int {
	tasks := make(map[int]int)

	for i := 0; i < n; i++ {
		tasks[i] = 0
	}

	return tasks
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files, trackFiles(len(files)), trackFiles(nReduce), []string{}, nReduce, make(map[int]string), 0, "Map"}

	c.server()
	return &c
}
