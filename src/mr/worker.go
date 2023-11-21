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

var NReduce int = 10

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// read contents, parses and use uders defined function
// produces key, values, buffer it in memory
// buffered writen to disk
// read data, sort

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

	// reply - contin wheher map/reduce, filename
	reply := CallAssignTask()

	fmt.Println("---------")
	fmt.Println(reply)
	fmt.Println("---------")

	if reply.IsMap {
		doMap(reply, mapf)
	} else {
		doReduce(reply, reducef)
	}

	// either map or reduce

	// reduce
	// sort -> group -> write to file
	// in (out_key, list_intermmediate_alues)
	// out -> output vlaue

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMap(reply Reply, mapf func(string, string) []KeyValue) {
	intermediate := make([][]KeyValue, NReduce)

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
	// hash the word -> put into bucket
	// intermediate = append(intermediate, kva...)
	for _, k := range kva {
		hashed := ihash(k.Key) % 10 // TODO: nReduce
		intermediate[hashed] = append(intermediate[hashed], k)
	}

	for i := 0; i < 10; i++ {
		bucket := intermediate[i]
		outName := "mr-X" + "-" + strconv.Itoa(i)
		file, _ := os.Create(outName)

		sort.Sort(ByKey(bucket))

		// encoding for smaller files
		enc := json.NewEncoder(file)
		for _, kv := range bucket {
			enc.Encode(&kv)
		}

		reply.Intermediate = append(reply.Intermediate, outName)
	}

	//fmt.Println(intermediate)

	fmt.Println("map" + reply.File)
}

func doReduce(reply Reply, reducef func(string, []string) string) {
	// reduce
	// sort -> group -> write to file
	// in (out_key, list_intermmediate_alues)
	// out -> output vlaue

	intermediate := reply.Intermediate
	var kva []KeyValue
	oname := "mr-out-" + strconv.Itoa(0)
	ofile, _ := os.Create(oname)

	file, err := os.Open(intermediate[0])
	if err != nil {
		log.Fatalf("cannot open %v", intermediate[0])
	}

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	i := 0
	for i < len(kva) {
		// load entried from the intermediate file

		// content loaded in kva now

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

		// do the reduce from mrseqential

		// save to the file output of mr-put-X?
	}

	// oname := "mr-out-0"
	// ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	// intermediate := reply.Intermediate
	// i := 0
	// for i < len(intermediate) {
	// 	j := i + 1
	// 	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
	// 		j++
	// 	}
	// 	values := []string{}
	// 	for k := i; k < j; k++ {
	// 		values = append(values, intermediate[k].Value)
	// 	}
	// 	output := reducef(intermediate[i].Key, values)

	// 	// this is the correct format for each line of Reduce output.
	// 	fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

	// 	i = j
	// }

	// ofile.Close()

	// fmt.Println("reduuce" + reply.File)

}

func CallAssignTask() Reply {
	args := ExampleArgs{}
	reply := Reply{}

	// will gets args and reply from coordinator?
	ok := call("Coordinator.AssignTasks", &args, &reply)
	if ok {
		fmt.Println("called")

	}

	return reply
	// if ok {
	// 	// reply.Y should be 100.
	// 	fmt.Printf("reply.Y %v\n", reply.Y)
	// } else {
	// 	fmt.Printf("call failed!\n")
	// }
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
