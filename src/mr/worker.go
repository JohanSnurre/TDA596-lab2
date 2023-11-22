package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key. (from mrsequential)
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

// main/mrworker.go calls this function.
// mapf, reducef - funciton in wc.go for mapreduce
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := CallAssignTask()

	if reply.IsMap {
		doMap(reply, mapf)
	} else {
		doReduce(reply, reducef)
	}
}

func doMap(reply Reply, mapf func(string, string) []KeyValue) {
	intermediate := make([][]KeyValue, reply.NReduce)

	// copied from mssequential for mapping
	filename := reply.File

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	// create nReduce intermediate files for consumption by the reduce tasks - nReduceXmap buckets?
	for _, k := range kva {
		hashed := ihash(k.Key) % reply.NReduce
		intermediate[hashed] = append(intermediate[hashed], k)
	}

	for i := 0; i < reply.NReduce; i++ {
		bucket := intermediate[i]
		outName := "mr-" + strconv.Itoa(reply.MapIndex) + "-" + strconv.Itoa(i)
		file, _ := os.Create(outName)

		// encoding for sjson
		enc := json.NewEncoder(file)
		for _, kv := range bucket {
			enc.Encode(&kv)
		}

		reply.Intermediate = append(reply.Intermediate, outName)
	}

	// msg coordinator that map done
	CallMapTaskFinished(reply)
}

func doReduce(reply Reply, reducef func(string, []string) string) {
	// intermediate := reply.Intermediate
	var kva []KeyValue
	oname := "mr-out-" + strconv.Itoa(reply.ReduceIndex)
	ofile, _ := os.Create(oname)

	for i := 0; i < reply.MapIndex; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceIndex)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	// copied from mrsquential
	i := 0
	for i < len(kva) {

		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

}

func CallAssignTask() Reply {
	args := ExampleArgs{}
	reply := Reply{}

	// will gets args and reply from coordinator?
	ok := call("Coordinator.AssignTasks", &args, &reply)
	if ok {
		fmt.Println("..")

	}

	return reply
}

func CallMapTaskFinished(r Reply) {
	args := MapArgs{}
	reply := Reply{}

	args.Intermediate = r.Intermediate
	args.MapIndex = r.MapIndex

	// will gets args and reply from coordinator?
	ok := call("Coordinator.ManageMapTaskFinished", &args, &reply)
	if ok {
		fmt.Println("############")

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
