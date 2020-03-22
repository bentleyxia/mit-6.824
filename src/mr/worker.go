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
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	id := new(WorkerID)
	id.AskID()

	for {
		filename := id.AskMapTask()
		if filename == "none" {
			break
		}
		fmt.Println(id.ID)
		fmt.Println(filename)
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
		sort.Sort(ByKey(kva))

		for i := 0; i < len(kva); {
			var oname string
			oname = fmt.Sprintf("mr-intermedia-%v", ihash(kva[i].Key)%10)
			fopen, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				fopen, _ = os.Create(oname)
			}
			// json write
			enc := json.NewEncoder(fopen)
			err = enc.Encode(&kva[i])
			j := i + 1
			for ; j < len(kva); j++ {
				if ihash(kva[i].Key) == ihash(kva[j].Key) {
					err = enc.Encode(&kva[j])
				} else {
					break
				}
			}
			i = j
			fopen.Close()
		}
		log.Println(id.AskMapState())

	}
	for !id.AskMapState() {
		time.Sleep(time.Second)
	}
	fmt.Println("reduce start")

	//reduce
	for {

		filename := id.AskReduceTask()
		log.Println(filename)
		f, err := os.Open(filename)
		if err != nil {
			log.Fatalln("cannot open file")
			break
		}
		dec := json.NewDecoder(f)
		intermediate := []KeyValue{}
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		oname := fmt.Sprintf("mr-out-%v", id.ID)
		ofile, _ := os.Create(oname)
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
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
		f.Close()

	}
}

//
// call Reduce on each distinct key in intermediate[],
// and print the result to mr-out-0.
//

// uncomment to send the Example RPC to the master.
//CallExample()

func (id *WorkerID) AskID() {
	args := new(WorkerID)
	reply := new(WorkerID)
	call("Master.AskID", &args, &reply)
	id.ID = reply.ID
}

//
func (id *WorkerID) AskMapTask() string {
	args := id
	reply := new(MapTaskReply)
	call("Master.MapTask", &args, &reply)
	id.File = reply.File
	return reply.File
}

func (id *WorkerID) AskMapState() bool {
	args := id
	reply := new(MapTaskReply)
	call("Master.MapTaskState", &args, &reply)
	return reply.Mapstate
}

func (id *WorkerID) AskReduceTask() string {
	args := id
	reply := new(ReduceTaskReply)
	call("Master.ReduceTask", &args, &reply)
	id.Reducenum = reply.
	return reply.FileR
}

func (id *WorkerID) AskReduceState() bool {
	args := id
	reply := new(ReduceTaskReply)
	call("Master.ReduceTaskState", &args, &reply)
	return reply.Reducestate
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
