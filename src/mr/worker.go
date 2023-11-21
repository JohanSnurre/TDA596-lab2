package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var group sync.WaitGroup

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func reduce(reducef func(string, []string) string, bucket int, workerID int, out *os.File) {

	/*

		1. Read the




	*/

	/*

		output := []KeyValue{}

		oname := "mr-out-" + strconv.Itoa(workerID) + "-" + strconv.Itoa(bucket)

		temp, err := os.ReadFile(oname)
		if err != nil {
			fmt.Println("kasdkasdksakd")
		}


		text := strings.Split(string(temp), "\n")
		firstLine := text[0]
		key := strings.Split(firstLine, " ")[0]
		for _, line := range text {
			t := strings.Split(line, " ")


		}

		i := 0
		for i < len(bucket) {
			j := i + 1
			for j < len(bucket) && bucket[j].Key == bucket[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, bucket[k].Value)
			}

			// this is the correct format for each line of Reduce output.
			output := reducef(bucket[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", bucket[i].Key, output)

			i = j
		}
		//c <- oname
		ofile.Close()


	*/
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	reply := getTaskCall()
	fileWC := mapf("LOL no", reply.Content)
	workerID := reply.WorkerID

	//sort.Sort(ByKey(fileWC))

	intermediateBuckets := make([][]KeyValue, reply.NReduce)
	for _, v := range fileWC {
		bucket := ihash(v.Key) % reply.NReduce

		intermediateBuckets[bucket] = append(intermediateBuckets[bucket], v)

	}

	for i := 0; i < reply.NReduce; i++ {
		oname := "mr-out-" + strconv.Itoa(workerID) + "-" + strconv.Itoa(i)
		file, err := os.Create(oname)
		if err != nil {
			fmt.Println("Error creating file!")
		}
		for _, v := range intermediateBuckets[i] {
			file.WriteString(v.Key + " " + v.Value + "\r\n")
		}

	}

	out, err := os.Create("mr-out-" + strconv.Itoa(workerID))
	if err != nil {
		fmt.Println("asdasd")
	}

	for i := 0; i < reply.NReduce; i++ {
		go reduce(reducef, i, workerID, out)
	}

	//c := make(chan string)
	/*for ind, val := range intermediateBuckets {
		group.Add(1)
		bucket := val
		go func(ind int) {

			defer group.Done()
			reduce(reducef, bucket, workerID, ind)

		}(ind)
	}*/

	group.Wait()

	fmt.Println("Done")
	/*for {
		fmt.Println(<-c)
	}
	*/
}

func getTaskCall() Reply {

	args := Args{}
	args.Command = "Give"

	reply := Reply{}

	ok := call("Coordinator.HandleWorker", &args, &reply)
	if !ok {
		fmt.Println("Call failed!")
	}

	fmt.Println("Got something to do!")

	return reply

	/*switch cmd := reply.Command; cmd {

	case "Map":

		ID := reply.WorkerID
		nReduce := reply.nReduce

		content := reply.Content

		m := mapf

		//do the motherfucking mapping


	default:
	}
	*/

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() string {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := StringReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.S)
	} else {
		fmt.Printf("call failed!\n")
	}
	r := reply.S
	return r

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

/*


	1. Ask the coordinator for some work. Use RPC and get the rask in the reply

	2. To the work on the received file/part of file

	3. Send rexults to coordinator





*/
