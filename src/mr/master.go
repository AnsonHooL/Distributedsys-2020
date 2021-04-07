package mr

import (
	"fmt"
	"log"
	"math"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"


type MasterPhase int

const (
	MapPhase    MasterPhase = 0
	ReducePhase MasterPhase = 1
)
const(
	MaxTasktime time.Duration = 10 * time.Second
	ScheTime    time.Duration = 500 * time.Millisecond
)

type TaskPhase int

const (
	Taskready   TaskPhase = 0
	Taskqueue   TaskPhase = 1
	Taskrunning TaskPhase = 2
	Taskdone    TaskPhase = 3
	Taskerror	TaskPhase = 4
)

///Master的结构
type Master struct {
	// Your definitions here.
	mu             sync.Mutex
	files          []string //输入文件列表
	nreduce        int
	nmap           int
	taskstatusList []TaskStatus //task任务状态列表
	masterphase    MasterPhase  //master工作阶段
	done           bool         //master完成状态
	taskchan       chan MRTask  //为什么不用指针
	wokerseq	   int          ///master为worker分配ID号
}

//记录任务的状态
type TaskStatus struct {
	starttime  time.Time   ///任务的开始时间
	workerid   int	      ///任务被分配到哪个worker的ID号
	taskphase  TaskPhase  ///任务的状态
}

///将Task的信息保存下来
func (m *Master) RegisTask(index int, args *GetOneTaskArgs){
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taskstatusList[index].workerid  = args.Workerid
	m.taskstatusList[index].starttime = time.Now()
	m.taskstatusList[index].taskphase = Taskrunning
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//


//Worker申请一个mapreduce-Task
func (m *Master) GetOneTask(args *GetOneTaskArgs, reply *GetOneTaskReply) error {
	if m.Done() {
		task := MRTask{
			Exitflag: true,
		}
		reply.MRTask = &task
		return nil
	}else{
		task:= <- m.taskchan
		reply.MRTask = &task

		index := reply.MRTask.Index
		m.RegisTask(index, args)
		return nil
	}
}

//worker申请自己的id号
func (m *Master) RegisWorker(args *RegisWorkerArgs, reply *RegisWorkerReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.wokerseq += 1
	reply.Wokerid = m.wokerseq

	return nil
}

///worker汇报工作完成
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	index := args.Index

	Dprint("report task: %+v, taskPhase: %+v", args, m.masterphase)
	///检查是否正确的worker完成任务
	if m.taskstatusList[index].workerid != args.Wokerid || m.masterphase != args.Masterphase {
		return nil
	}

	if args.Finish {
		m.taskstatusList[index].taskphase = Taskdone
	}else {
		m.taskstatusList[index].taskphase = Taskerror
	}

	go m.polling() //迅速检查是否完成作业
	return nil
}

func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}



//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m) //注册一个Master类方法
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}




//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Your code here.
	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func (m *Master) GetTask(index int) MRTask{

	re := MRTask{
		Index:       index,
		Nreduce:     m.nreduce,
		Nmap:        m.nmap,
		Masterphase: m.masterphase,
		Exitflag:    false,
	}
	if m.masterphase == MapPhase {
		re.Filename = m.files[index]
	}
	//Dprint("GetTask:\nm:%+v, index:%d, lenfiles:%d, lents:%d", m, index, len(m.files), len(m.taskstatusList))
	Dprint("GetTask:\nindex:%d, lenfiles:%d, lents:%d", index, len(m.files), len(m.taskstatusList))
	return re
}

///一个轮询，Master去检查所有工作是否做完
func (m *Master) polling() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}

	allworkdone := true
	for index,task := range m.taskstatusList{
		switch task.taskphase {
		case Taskready:
			allworkdone = false
			m.taskchan <- m.GetTask(index)
			m.taskstatusList[index].taskphase = Taskqueue
		case Taskqueue:
			allworkdone = false
		case Taskrunning:
			allworkdone = false
			if time.Now().Sub(task.starttime) > MaxTasktime{
				Dprint("Over Time!!!!!!!!!!!!\n")
				m.taskstatusList[index].taskphase = Taskqueue
				m.taskchan <- m.GetTask(index)
				m.taskstatusList[index].workerid = -1
			}
		case Taskdone:
		case Taskerror:
			allworkdone = false
			m.taskstatusList[index].taskphase = Taskqueue
			m.taskchan <- m.GetTask(index)
			m.taskstatusList[index].workerid = -1
		default:
			fmt.Print("polling fail\n")
		}
	}

	if allworkdone {
		if m.masterphase == MapPhase {
			m.taskstatusList = make([]TaskStatus, m.nreduce)
			m.masterphase = ReducePhase
			Dprint("Mapphase ---> Reducephase.\n")
		}else {
			m.done = true
		}
	}

}

func (m *Master) tickSchedule() {
	// 按说应该是每个 task 一个 timer，此处简单处理
	for !m.Done() {
		go m.polling()
		time.Sleep(ScheTime)
	}
}


func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mu = sync.Mutex{}
	m.files = files
	m.nreduce = nReduce
	m.nmap = len(files)
	m.taskstatusList = make([]TaskStatus, len(files)) //先按map任务数量分配大小
	m.masterphase = MapPhase
	m.done = false
	m.taskchan = make(chan MRTask, int32(math.Max(float64(m.nreduce), float64(m.nmap))))

	// Your code here.

	go m.tickSchedule()
	m.server()
	Dprint("master init")
	return &m
}



