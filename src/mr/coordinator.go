package mr

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
	availableIDs  []int
	stage         string
	activeWorkers int
}

var mutex sync.Mutex
var group2 sync.WaitGroup

var t = 10

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *StringReply) error {

	filename := "../main/pg-grimm.txt"

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	reply.S = string(content)
	return nil
}

func (c *Coordinator) HandleWorker(args *Args, reply *Reply) error {

	switch cmd := args.Command; cmd {

	case "Give":

		mutex.Lock()
		stage := c.stage
		mutex.Unlock()

		if stage == "Map" {

			mutex.Lock()
			if len(c.files) == 0 {
				reply.Command = "Sleep"
				mutex.Unlock()
				break
			}
			workerID := c.availableIDs[0]
			c.availableIDs = c.availableIDs[1:]
			//fmt.Println("MAP ID: " + strconv.Itoa(workerID))
			c.lastGivenID = workerID + 1
			filename := c.files[0]
			c.files = c.files[1:]
			reply.Command = c.stage

			c.fileWorker[workerID] = filename
			//fmt.Println("MAP GAVE OUT ID: ", workerID)
			mutex.Unlock()

			reply.WorkerID = workerID
			reply.NReduce = c.nReduce
			reply.Content = filename

			go c.asyncCheck(t, workerID, "Map")

		} else if stage == "Reduce" {

			mutex.Lock()

			if len(c.intermediates) == 0 {
				reply.Command = "Sleep"
				mutex.Unlock()
				break
			}

			filename := c.intermediates[0]
			c.intermediates = c.intermediates[1:]
			temp := strings.Split(filename, "-")
			workerID, _ := strconv.Atoi(temp[3]) //c.availableIDs[0]
			c.availableIDs = c.availableIDs[1:]
			//fmt.Println("REDUCE ID: " + strconv.Itoa(workerID))
			c.lastGivenID = workerID + 1
			reply.Command = c.stage

			mutex.Unlock()

			reply.WorkerID = workerID
			reply.NReduce = c.nReduce

			reply.Content = filename

			mutex.Lock()
			c.fileWorker[workerID] = filename
			//fmt.Println("REDUCE GAVE OUT ID: ", workerID, " WITH FILE ", filename)
			mutex.Unlock()

			go c.asyncCheck(t, workerID, "Reduce")

		}

	case "Done Mapping":
		mutex.Lock()

		delete(c.fileWorker, args.WorkerID)
		if len(c.files) == 0 && len(c.fileWorker) == 0 {
			c.lastGivenID = 0
			c.stage = "Reduce"
			for i := 0; i < c.nReduce; i++ {
				filename := "mr-out-*-" + strconv.Itoa(i)
				//fmt.Println(filename)
				c.intermediates = append(c.intermediates, filename)
				c.availableIDs = append(c.availableIDs, i)
			}
			//reply.Command = "Well done"
		}
		mutex.Unlock()

	case "Done":
		mutex.Lock()

		if _, ok := c.fileWorker[args.WorkerID]; !ok {
			reply.Command = "TOO LATE YOU SLOW POS"
			mutex.Unlock()
			break
		}
		delete(c.fileWorker, args.WorkerID)
		if len(c.intermediates) == 0 && len(c.fileWorker) == 0 {
			c.lastGivenID = 0
			c.stage = "Done"
		}

		reply.Command = "Well done"
		mutex.Unlock()

	default:

	}

	return nil

}

func (c *Coordinator) asyncCheck(sleepSeconds int, workerID int, stage string) {
	time.Sleep(time.Duration(sleepSeconds) * time.Second)
	mutex.Lock()

	switch coordStage := c.stage; coordStage {

	case "Map":
		if stage != "Map" {
			break
		}
		if file, ok := c.fileWorker[workerID]; ok {
			//fmt.Println(strconv.Itoa(workerID) + " has crashed working on " + c.fileWorker[workerID])
			c.files = append(c.files, file)
			delete(c.fileWorker, workerID)
			c.availableIDs = append(c.availableIDs, workerID)
			//fmt.Println(c.availableIDs)
			//fmt.Println("ADDED ID " + strconv.Itoa(workerID) + " TO AVAILABLEIDS")
			filename := "mr-out-" + strconv.Itoa(workerID) + "-*"
			for i := 0; i < c.nReduce; i++ {
				filename := strings.Replace(filename, "*", strconv.Itoa(i), 1)
				os.Remove(filename)
				//fmt.Println("REMOVING " + filename)
			}
		}

	case "Reduce":
		if stage != "Reduce" {
			break
		}
		if file, ok := c.fileWorker[workerID]; ok {
			//fmt.Println(strconv.Itoa(workerID) + " has crashed working on " + c.fileWorker[workerID])
			c.intermediates = append(c.intermediates, file)
			//fmt.Println("RETURNED " + file + " TO INTERMEDIATES")
			//fmt.Println(c.intermediates)
			//filename := c.fileWorker[workerID]
			//fmt.Println("FILENAME: " + filename)
			c.availableIDs = append(c.availableIDs, workerID)
			//fmt.Println("ADDED ID " + strconv.Itoa(workerID) + " TO AVAILABLEIDS")
			//fmt.Println(c.availableIDs)
			delete(c.fileWorker, workerID)
			filename := "mr-out-" + strconv.Itoa(workerID)
			os.Remove(filename)
			//fmt.Println("REMOVING " + filename)
		}

	}

	mutex.Unlock()
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
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

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var availableIDs []int
	for i := 0; i < len(files); i++ {
		availableIDs = append(availableIDs, i)
	}
	c := Coordinator{files, []string{}, nReduce, make(map[int]string), 0, availableIDs, "Map", 0}

	c.server()
	return &c
}

/*
	1. Listen for incomming work requests from workers

	2. Find some work and attach it to the reply struct in the RPC reply

	3. Give the worker some time to finish their work, if the work isn't done in a reasonable time then assign the work to someone else.

	4. Receive response from workers, combine results into a output file.




*/
