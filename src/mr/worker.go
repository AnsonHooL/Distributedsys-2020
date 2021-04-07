package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	workerid  int
	mapf      func(string, string) []KeyValue
	reducef   func(string, []string) string
	task	  *MRTask
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}



func (w *worker) doMapwork() {

	filename := w.task.Filename
	file, err := os.Open(filename)
	if err != nil {
		w.Callreporttask(false)
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		w.Callreporttask(false)
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	start := time.Now()
	kva := w.mapf(filename, string(content))
	dura := time.Now().Sub(start)

	if dura > time.Second * 8{
		w.Callreporttask(false)
		return
	}

	reduces := make([][]KeyValue, w.task.Nreduce)

	for _, kv := range kva {
		idx := ihash(kv.Key) % w.task.Nreduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces{
		fileName := reduceTmpName(w.task.Index, idx) ///前面的是map的编号，后面的是reduce的编号

		f, err := os.Create(fileName)
		if err != nil {
			w.Callreporttask(false)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				w.Callreporttask(false)
				return
			}
		}

		if err := f.Close(); err != nil {
			w.Callreporttask(false)
		}
	}

	for i:=0; i < w.task.Nreduce; i++{
		oldpath:= reduceTmpName(w.task.Index, i)
		newpath:= reduceName(w.task.Index, i)
		//f, err := os.Open(oldpath)
		if err != nil {
			w.Callreporttask(false)
			return
		}
		if err := os.Rename(oldpath, newpath); err != nil {
			w.Callreporttask(false)
		}

		//if err := f.Close(); err != nil {
		//	w.Callreporttask(false)
		//}
	}

	w.Callreporttask(true)
}

func (w *worker) doReducework(){
	mymap := make(map[string][]string)
	for i:=0; i < w.task.Nmap; i++ {

		fileName := reduceName(i, w.task.Index)

		file, err := os.Open(fileName)

		if err != nil {
			w.Callreporttask( false)
			Dprint("Reduce work can not open :%v",fileName)
			return
		}

		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := mymap[kv.Key]; !ok {
				mymap[kv.Key] = make([]string, 0, 100)
			}
			mymap[kv.Key] = append(mymap[kv.Key], kv.Value)
		}
	}
	res := make([]string, 0, 100)
	for k, v := range mymap {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	///将所有输出连起来
	if err := ioutil.WriteFile(mergetmpName(w.task.Index), []byte(strings.Join(res, "")), 0600); err != nil {
		w.Callreporttask(false)
	}

	oldpath:= mergetmpName(w.task.Index)
	newpath:= mergeName(w.task.Index)

	if err := os.Rename(oldpath, newpath); err != nil {
		w.Callreporttask(false)
	}

	w.Callreporttask(true)
}

func (w *worker) workerrun() {
	for {

		w.Callgetonetask()

		if w.task.Exitflag{
			Dprint("Job done : Woker should exit\n")
			os.Exit(0)
		}

		if w.task.Masterphase == MapPhase {
			w.doMapwork()
		}else if w.task.Masterphase == ReducePhase {
			w.doReducework()
		}

	}
}



//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{
		mapf: mapf,
		reducef: reducef,
	}

	w.Callregisworker()

	w.workerrun()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func (w *worker) Callregisworker(){
	args := RegisWorkerArgs{}
	reply := RegisWorkerReply{}

	if ok := call("Master.RegisWorker", &args, &reply); !ok {
		Dprint("regis worker fail, exit")
		os.Exit(1)
	}

	w.workerid = reply.Wokerid

}


func (w *worker) Callgetonetask(){
	args  := GetOneTaskArgs{
		Workerid: w.workerid,
	}
	reply := GetOneTaskReply{}

	if ok := call("Master.GetOneTask", &args, &reply); !ok {
		Dprint("worker get task fail,exit")
		os.Exit(1)
	}
	w.task = reply.MRTask
	Dprint("Worker get one task \n")
}


func (w *worker) Callreporttask(flag bool){
	args  := ReportTaskArgs{
		Wokerid: w.workerid,
		Index: w.task.Index,
		Finish: flag,
		Masterphase: w.task.Masterphase,
	}

	reply := ReportTaskReply{
	}

	if ok := call("Master.ReportTask", &args, &reply); !ok {
		Dprint("worker report task fail,exit")
		os.Exit(1)
	}
}

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
