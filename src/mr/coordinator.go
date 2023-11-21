package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.

	files       []string
	nReduce     int
	fileWorker  map[string]int
	lastGivenID int
}

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

	workerID := c.lastGivenID + 1

	switch cmd := args.Command; cmd {
	case "Give":
		//path := "./main/"
		filename := c.files[0]
		c.files = c.files[1:]

		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open!! %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		reply.WorkerID = workerID
		reply.NReduce = c.nReduce
		fmt.Println(c.nReduce)
		reply.Command = "Map"
		reply.Content = string(content)

		c.fileWorker[filename] = workerID
		fmt.Println("Given out: ", filename)

		/*
			time.sleep(10 seconds)
			if the file associated with the worker hasnt reported done then select
			a new worker for that file

		*/

	default:

	}

	c.lastGivenID = workerID
	return nil

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

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files, nReduce, make(map[string]int), 0}

	// Your code here.

	c.server()
	return &c
}

/*
	1. Listen for incomming work requests from workers

	2. Find some work and attach it to the reply struct in the RPC reply

	3. Give the worker some time to finish their work, if the work isn't done in a reasonable time then assign the work to someone else.

	4. Receive response from workers, combine results into a output file.




*/
