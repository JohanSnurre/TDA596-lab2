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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var downloader *s3manager.Downloader
var uploader *s3manager.Uploader

const Fs = "ds-2-zz-1123"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)
	if err != nil {
		panic(err)
	}

	downloader = s3manager.NewDownloader(sess)
	uploader = s3manager.NewUploader(sess)

	for {
		args := Args{-1, "Give", "", ""}
		reply := getTaskCall(&args)

		switch task := reply.Command; task {

		case "Map":
			//fmt.Println("satrted: " + strconv.Itoa(reply.WorkerID))
			args = doMap(reply, mapf)
			//fmt.Println("------->: " + strconv.Itoa(reply.WorkerID))
			reply = getTaskCall(&args)
		case "Reduce":
			args := doReduce(reply, reducef)
			reply = getTaskCall(&args)

		case "Sleep":
			//fmt.Println("I am sleeping for 10 seconds!")
			time.Sleep(1 * time.Second)

		case "Well done", "Please die":
			return
		default:
			return

		}
	}

}

func doMap(reply Reply, mapf func(string, string) []KeyValue) Args {
	nReduce := reply.NReduce
	workerID := reply.WorkerID

	f, err := os.Create(reply.Content)
	if err != nil {
		fmt.Println("Error creating file locally")
		return Args{}
	}

	_, err = downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String(Fs),
		Key:    aws.String(reply.Content),
	})

	if err != nil {
		fmt.Println("Error retreiving file from cloud")
		return Args{}
	}

	// get file content
	file, err := os.Open(reply.Content)
	if err != nil {
		log.Fatalf("cannot open!! %v", reply.Content)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Content)
	}
	file.Close()

	// do mapping
	fileWC := mapf(file.Name(), string(content))

	// add to intermediates
	intermediateBuckets := make([][]KeyValue, reply.NReduce)
	for _, v := range fileWC {
		bucket := ihash(v.Key) % nReduce

		intermediateBuckets[bucket] = append(intermediateBuckets[bucket], v)
	}

	// save intermediate files
	for i := 0; i < nReduce; i++ {
		oname := "mr-o-" + strconv.Itoa(workerID) + "-" + strconv.Itoa(i)
		file, err := os.Create(oname)
		if err != nil {
			log.Fatalf("Error creating file!")
		}
		enc := json.NewEncoder(file)
		for _, v := range intermediateBuckets[i] {
			err := enc.Encode(&v)
			if err != nil {
				log.Fatalf("ERROR ENCODING")
			}
		}

		f, err := os.Open(oname)
		if err != nil {
			panic("Error opening file")
		}

		_, err = uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(Fs),
			Key:    aws.String(oname),
			Body:   f,
		})
		if err != nil {
			panic("Error uploading intermediate to cloud")
		}
	}

	return Args{workerID, "Done Mapping", "mr-o-" + strconv.Itoa(workerID), ""}
}

func doReduce(reply Reply, reducef func(string, []string) string) Args {
	workerID := reply.WorkerID
	//nReduce := reply.NReduce
	filename := reply.Content

	// create output file
	out, err := os.Create("mr-out-" + strconv.Itoa(workerID))
	if err != nil {
		log.Fatalf("ERROR CREATING OUTPUT FILE")
	}

	// reduce
	var intermediate []KeyValue

	for p := 0; p <= 7; p++ { // should be 7?>>?>
		oname := strings.Replace(filename, "*", strconv.Itoa(p), 1)

		f, err := os.Create(oname)
		if err != nil {
			panic("Error creating reduce ouput file")
		}

		fmt.Println(oname)

		_, err = downloader.Download(f, &s3.GetObjectInput{
			Bucket: aws.String(Fs),
			Key:    aws.String(oname),
		})

		file, err := os.Open(oname)
		if err != nil {
			break
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}

			intermediate = append(intermediate, kv)

		}
		file.Close()

		//os.Remove(oname)
	}

	// sorting

	sort.Sort(ByKey(intermediate))

	// reduce from mrsquential
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(out, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	//WAIT FOR REDUCING TO BE DONE
	out.Close()

	//fmt.Println("Reducing done")
	args := Args{}
	reply = Reply{}

	args.WorkerID = workerID
	args.Command = "Done"

	return args
}

func getTaskCall(args *Args) Reply {

	reply := Reply{}

	ok := call("Coordinator.HandleWorker", &args, &reply)
	if !ok {
		//fmt.Println("Call failed!")
		return reply
	}

	return reply

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
