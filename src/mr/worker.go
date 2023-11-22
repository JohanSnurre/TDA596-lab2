package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type Bykey []KeyValue

// Less implements sort.Interface.
func (a Bykey) Less(i int, j int) bool {
	return a[i].Key < a[j].Key
}

// Swap implements sort.Interface.
func (a Bykey) Swap(i int, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a Bykey) Len() int { return len(a) }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		// ask for a task
		//tasktype will be decided by the coordinator
		task := TaskReply{}

		if task.TaskType == "map" {
			intermediate := []KeyValue{}

			file, err := os.Open(task.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", task.FileName)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.FileName)
			}
			file.Close()

			kv := mapf(task.FileName, string(content))
			//append to intermediate
			intermediate = append(intermediate, kv...)

			//hash into nReduce buckets
			buckets := make([][]KeyValue, task.NReduce)
			//initialize buckets
			for i := 0; i < task.NReduce; i++ {
				buckets[i] = []KeyValue{}
			}
			for _, kv := range intermediate {
				//use ihash(key) % NReduce to choose the reduce
				bucket := ihash(kv.Key) % task.NReduce
				buckets[bucket] = append(buckets[bucket], kv)
			}

			//write to intermediate files
			for i := 0; i < task.NReduce; i++ {
				oname := "mr-" + strconv.Itoa(task.maptasknum) + "-" + strconv.Itoa(i)
				ofile, _ := os.CreateTemp("", oname+"*")
				enc := json.NewEncoder(ofile)
				for _, kv := range buckets[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode %v", kv)
					}
				}
				os.Rename(ofile.Name(), oname)
				ofile.Close()
			}
			//finished_FinishTask := TaskArgs{task.maptasknum, -1}
			//finished_AskTask := ExampleReply{}

		} else if task.TaskType == "reduce" {
			// reduce task
			intermediate := []KeyValue{}
			//read from intermediate files
			for i := 0; i < task.nMap; i++ {
				Fname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.reducetasknum)
				file, err := os.Open(Fname)
				if err != nil {
					log.Fatalf("cannot open %v", Fname)
				}
				//the hint says to use json decoder
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err == io.EOF {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			//sort by key
			sort.Sort(Bykey(intermediate))

			//write to output file
			oname := "mr-out-" + strconv.Itoa(task.reducetasknum)
			ofile, _ := os.CreateTemp("", oname+"*")

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}

				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			os.Rename(ofile.Name(), oname)
			ofile.Close()

			for i := 0; i < task.nMap; i++ {
				Fname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.reducetasknum)
				err := os.Remove(Fname)
				if err != nil {
					log.Fatalf("cannot remove %v", Fname)
				}
			}

			//finished_FinishTask := TaskArgs{-1, task.reducetasknum}
			//finished_AskTask := ExampleReply{}

		} else {
			// no task
			// sleep
		}

	}

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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
