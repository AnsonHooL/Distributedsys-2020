package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"log"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
const Debug = true

func Dprint(format string, v ...interface{}){
	if Debug {
		log.Printf(format, v...)
	}
}





type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RegisterArgs struct {
	X int
}

type RegisterReply struct {
	Y int
}


type GetOneTaskArgs struct {
	workerid int
}

type GetOneTaskReply struct {
	MRTask      *MRTask ///返回一个MapReduce-Task
}

type RegisWorkerArgs struct {

}

type RegisWorkerReply struct {
	wokerid  int ///返回一个workerID
}

//Mapreduce的Task结构
type MRTask struct {
	index        int ///这个任务在Master里面的索引
	Filename     string ///输入任务的input文件
	masterphase  MasterPhase ///Master的状态，任务是map还是reduce
	Nmap         int
	Nreduce      int
}



// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
