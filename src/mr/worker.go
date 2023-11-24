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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var group sync.WaitGroup

var downloader *s3manager.Downloader
var uploader *s3manager.Uploader
var GFSName string

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

func reduce(reducef func(string, []string) string, filename string, nReduce int, out *os.File) {

	/*

		1. Read the contents of the intermediate file
		2. Convert each line into a KeyValue struct and put them in memory inside a slice
		3. Sort this slice by using sort.Sort(ByKey(fileWC))
		4. Calculate the reduce of each key, which is unique for that file, and store it in some output struct
		5. Write to the output filezel




	*/
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	if err != nil {
		panic("ERROR REDUCE asd")
	}
	svc := s3.New(sess)

	var intermediate []KeyValue

	for p := 0; p <= nReduce; p++ {
		//fmt.Printf("Worker: %d, Bucket: %s\n", p, filename)
		//oname := "mr-out-" + strconv.Itoa(p) + "-" + strconv.Itoa(bucket)
		//fmt.Println(oname)

		/*temp, err := os.ReadFile(oname)
		if err != nil {
			//fmt.Println("kasdkasdksakd")
		}
		*/

		oname := strings.Replace(filename, "*", strconv.Itoa(p), 1)

		f, err := os.Create(oname)
		if err != nil {
			panic("Error creating reduce ouput file")
		}

		_, err = downloader.Download(f, &s3.GetObjectInput{
			Bucket: aws.String(GFSName),
			Key:    aws.String(oname),
		})
		if err != nil {
			os.Remove(oname)
			break
			//panic("Error downloading file from cloud in reduce")
		}

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

		/*text := strings.Split(string(temp), "\n")

		for _, line := range text {
			if line == "" {
				break
			}
			t := strings.Split(line, " ")
			intermediate = append(intermediate, KeyValue{t[0], t[1]})

		}
		*/
		file.Close()

		fmt.Println(oname)

		input := &s3.DeleteObjectInput{
			Bucket: aws.String(GFSName),
			Key:    aws.String(oname),
		}
		_, err = svc.DeleteObject(input)
		if err != nil {
			panic("Error removing intermediate from cloud")
		}

		os.Remove(oname)
	}

	sort.Sort(ByKey(intermediate))

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
		//fmt.Println(intermediate[i].Key, len(values))
		// this is the correct format for each line of Reduce output.
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(out, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	out.Close()

	temp, err := os.Open(out.Name())
	if err != nil {
		fmt.Println("Error uploading final file")
	}
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(GFSName),
		Key:    aws.String(out.Name()),
		Body:   temp,
	})
	if err != nil {
		fmt.Println("Error uploading final file")
	}
	temp.Close()
	os.Remove(out.Name())
	//c <- oname
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.

	for {

		args := Args{-1, "Give", "", ""}
		reply := getTaskCall(&args)
		workerID := reply.WorkerID
		nReduce := reply.NReduce
		GFSName = reply.GFSname

		sess, err := session.NewSession(&aws.Config{
			Region: aws.String("us-east-1")},
		)
		if err != nil {
			panic(err)
		}

		//svc := s3.New(sess)
		downloader = s3manager.NewDownloader(sess)
		uploader = s3manager.NewUploader(sess)

		switch task := reply.Command; task {

		case "Map":

			f, err := os.Create(reply.Content)
			if err != nil {
				fmt.Println("Error creating file locally")
				return
			}
			fmt.Println(GFSName)

			_, err = downloader.Download(f, &s3.GetObjectInput{
				Bucket: aws.String(GFSName),
				Key:    aws.String(reply.Content),
			})
			if err != nil {
				fmt.Println("Error retreiving file from cloud")
				return
			}

			//fmt.Println("I am worker", workerID, "with PID:", os.Getpid())
			//fmt.Println("Working on mapping")
			//MAPPING

			file, err := os.Open(reply.Content)
			if err != nil {
				log.Fatalf("cannot open!! %v", reply.Content)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Content)
			}
			file.Close()

			fileWC := mapf(file.Name(), string(content))

			//sort.Sort(ByKey(fileWC))

			intermediateBuckets := make([][]KeyValue, reply.NReduce)
			for _, v := range fileWC {
				bucket := ihash(v.Key) % nReduce

				intermediateBuckets[bucket] = append(intermediateBuckets[bucket], v)

			}

			for i := 0; i < nReduce; i++ {
				oname := "mr-out-" + strconv.Itoa(workerID) + "-" + strconv.Itoa(i)
				file, err := os.Create(oname)
				if err != nil {
					//fmt.Println("Error creating file!")
				}
				enc := json.NewEncoder(file)
				for _, v := range intermediateBuckets[i] {
					err := enc.Encode(&v)
					//file.WriteString(v.Key + " " + v.Value + "\n")
					if err != nil {
						//fmt.Println("ERROR ENCODING")
					}
				}
				f, err := os.Open(oname)
				if err != nil {
					panic("Error opening file")
				}
				_, err = uploader.Upload(&s3manager.UploadInput{
					Bucket: aws.String(GFSName),
					Key:    aws.String(oname),
					Body:   f,
				})
				if err != nil {
					panic("Error uploading intermediate to cloud")
				}
			}
			args = Args{workerID, "Done Mapping", "mr-out-" + strconv.Itoa(workerID) + "-*", ""}
			reply = getTaskCall(&args)

		case "Reduce":
			//fmt.Println("Dispatching reducers")
			//REDUCING
			output, err := os.Create("mr-out-" + strconv.Itoa(workerID))
			if err != nil {
				//fmt.Println("ERROR CREATING OUTPUT FILE")
			}

			reduce(reducef, reply.Content, nReduce, output)

			//WAIT FOR REDUCING TO BE DONE
			output.Close()

			//fmt.Println("Reducing done")
			args = Args{}
			reply = Reply{}

			args.WorkerID = workerID
			args.Command = "Done"
			args.ContentName = "mr-out-" + strconv.Itoa(workerID)
			output, err = os.Open("mr-out-" + strconv.Itoa(workerID))
			if err != nil {
				//fmt.Println("ERROR")
			}
			text, err := io.ReadAll(output)
			if err != nil {
				//fmt.Println("ERROR")
			}
			args.Content = string(text)

			reply = getTaskCall(&args)

		case "Sleep":
			//fmt.Println("I am sleeping for 10 seconds!")
			time.Sleep(1 * time.Second)

		case "Well done", "Please die":
			//fmt.Println("EXITING")
			return
		default:
			//fmt.Println("No response from RPC server, I am now dead!")
			return

		}

		////fmt.Println("Answer from coordinator: ", reply.Command)

	}

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
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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
