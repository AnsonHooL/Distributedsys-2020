package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const(
	MaxLockTime        time.Duration = 10  * time.Millisecond
	MaxWaitopTime	   time.Duration = 200 * time.Millisecond
	ChangeLeaderTime   time.Duration = 20  * time.Millisecond
)



const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqcmdID   int64
	ClientId   int64
	Key        string
	Method     string
	Value	   string
}

type ClerkResult struct {
	Cmdseqid  int64
	Value     string
	Error     Err
}

func (kv *KVServer) lock(m string) {
	kv.mu.Lock()
	kv.lockStart = time.Now()
	kv.lockName = m
}

func (kv *KVServer) unlock(m string) {
	kv.lockEnd = time.Now()
	duration := kv.lockEnd.Sub(kv.lockStart)
	kv.lockName = ""
	kv.mu.Unlock()
	if duration > MaxLockTime {
		DPrintf(fmt.Sprintf("lock too long:%s:%s\n", m, duration))
	}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	stopCh  chan struct{}

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore        map[string]string
	applyHistory   map[int64]int64 //保存client对应最新的seq-id，用来过滤重复请求.key是clientid，value是seqid
	resultCache	   map[int64]ClerkResult //保存client的结果，key是clientid，value是result
	persister      *raft.Persister

	//Debug
	lockStart	    time.Time
	lockEnd			time.Time
	lockName		string

}

func (kv *KVServer) waitopreply(op Op)(re ClerkResult){
	index,term,isLeader := kv.rf.Start(op)

	DPrintf("server:%d, wait an op:%v,index:%d,term:%d.clinet id:%d,segid:%d",kv.me,op.Method,index,term,op.ClientId,op.SeqcmdID)

	if !isLeader{
		re.Error = ErrWrongLeader
		return
	}

	waittime := MaxWaitopTime / 100

	for i := 0; i < 100; i++{
		time.Sleep(waittime)
		kv.lock("waitop")
		if op.SeqcmdID == kv.applyHistory[op.ClientId]{
			re.Error = kv.resultCache[op.ClientId].Error
			re.Value = kv.resultCache[op.ClientId].Value
			re.Cmdseqid = kv.resultCache[op.ClientId].Cmdseqid
			kv.unlock("waitop")
			return
		}
		kv.unlock("waitop")
	}

	re.Error = TimeOut
	return
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_,isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("kvserver iskilled:%v", kv.killed())
		reply.Err = ErrWrongLeader
		return
	}

	kv.lock("Get method")
	hiscmdID,ok := kv.applyHistory[args.ClientID]

	if ok == false { //第一次查询，如果并不存在历史，先存一条数据
		hiscmdID = 0
		kv.applyHistory[args.ClientID] = hiscmdID
	}

	if hiscmdID < args.SeqcmdID{ //来的是新的op
		op := Op{
			SeqcmdID:     args.SeqcmdID,
			Key:          args.Key,
			Method:       Getstr,
			ClientId:     args.ClientID,
		}
		kv.unlock("Get method")

		re := kv.waitopreply(op)

		reply.Value = re.Value
		reply.Err   = re.Error

		return
	}else if hiscmdID == args.SeqcmdID{

		reply.Err   = kv.resultCache[args.ClientID].Error
		reply.Value = kv.resultCache[args.ClientID].Value

		if kv.resultCache[args.ClientID].Cmdseqid != args.SeqcmdID{
			DPrintf("Request not match.")
			os.Exit(-1)
		}

		kv.unlock("Get method")
		return

	}else { //来的是旧的op,有问题
		DPrintf("Can not handle old op request.")
		os.Exit(-1)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_,isLeader := kv.rf.GetState()

	if !isLeader {
		DPrintf("kvserver iskilled:%v", kv.killed())
		reply.Err = ErrWrongLeader
		return
	}

	kv.lock("PutAppend method")

	hiscmdID,ok := kv.applyHistory[args.ClientID]

	if ok == false { //第一次查询，如果并不存在历史，先存一条数据
		hiscmdID = 0
		kv.applyHistory[args.ClientID] = hiscmdID
	}

	if hiscmdID < args.SeqcmdID{ //来的是新的op
		op := Op{
			SeqcmdID:     args.SeqcmdID,
			Key:          args.Key,
			Method:       args.Op,
			ClientId:     args.ClientID,
			Value: 		  args.Value,
		}
		kv.unlock("PutAppend method")

		re := kv.waitopreply(op)
		reply.Err   = re.Error
		return
	}else if hiscmdID == args.SeqcmdID{

		reply.Err   = kv.resultCache[args.ClientID].Error

		if kv.resultCache[args.ClientID].Cmdseqid != args.SeqcmdID{
			DPrintf("Request not match.")
			os.Exit(-1)
		}

		kv.unlock("Get method")
		return

	}else { //来的是旧的op,有问题
		DPrintf("Can not handle old op request.")
		os.Exit(-1)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.stopCh)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readPersist(data []byte){
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	DPrintf("read Persist a kvserver:%d.", kv.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData       map[string]string
	var applyhistory map[int64]int64
	var resultcache  map[int64]ClerkResult
	if d.Decode(&kvData) != nil ||
		d.Decode(&applyhistory) != nil ||
			d.Decode(&resultcache)!= nil {
		log.Fatal("kv read persist err")
	} else {
		kv.kvStore      = kvData
		kv.applyHistory = applyhistory
		kv.resultCache  = resultcache
	}
}


func (kv *KVServer) dataGet(key string) (err Err, val string) {
	if v, ok := kv.kvStore[key]; ok {
		err = OK
		val = v
		return
	} else {
		err = ErrNoKey
		val = ""
		return
	}
}


func (kv *KVServer) genSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.kvStore); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.applyHistory); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.resultCache); err != nil {
		panic(err)
	}
	data := w.Bytes()
	return data
}

func (kv *KVServer) savesnapshot(logIndex int){
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	//需要做快照
	DPrintf("raft: %d,Save snapshot!!!!!!!!!!!!!!!!!!!!!!", kv.me)
	data := kv.genSnapshotData()
	//time.Sleep(100 * time.Millisecond)
	kv.rf.SavePersistAndShnapshot(logIndex, data)
}

func (kv *KVServer) waitapply(){

	for{
		select {
		case <-kv.stopCh:
			DPrintf("stop ch get")
			return
		case msg := <- kv.applyCh:
			if !msg.CommandValid{
				DPrintf("Cmmmand valid:%v",msg.CommandValid)

				kv.lock("read persist")
				kv.readPersist(kv.persister.ReadSnapshot())
				kv.unlock("read persisit")

			}else {
				op := msg.Command.(Op)
				clientid := op.ClientId
				opid     := op.SeqcmdID

				kv.lock("read state machine")

				DPrintf("server:%d,get apply clinet id:%d seq id%d, kv.applyHistory[clientid]:%d.\nmsgidx:%d", kv.me ,clientid, opid, kv.applyHistory[clientid],
					msg.CommandIndex)

				if kv.applyHistory[clientid] == opid{
					//重复的命令
					kv.unlock("read state machine")
					continue
				}else if kv.applyHistory[clientid] < opid{ //状态机只会被这里修改.  来的是新的命令

					_,ok := kv.applyHistory[clientid]
					DPrintf("server:%d, key in map:%v,", kv.me, ok)

					if kv.applyHistory[clientid] + 1 != opid{
						_,ok := kv.applyHistory[clientid]

						DPrintf("apply fail!!! server:%d, kv.applyHistory[clientid]:%d.key in map:%v",kv.me, kv.applyHistory[clientid],ok)

					}


					result   := ClerkResult{
						Cmdseqid: opid,
					}

					switch op.Method {
					case Getstr:
						err, v := kv.dataGet(op.Key)
						result.Error = err
						result.Value = v
					case Appendstr:
						_, v := kv.dataGet(op.Key)
						kv.kvStore[op.Key] = v + op.Value
						result.Error = OK
					case Putstr:
						kv.kvStore[op.Key] = op.Value
						result.Error = OK
					}

					kv.applyHistory[clientid] = opid
					kv.resultCache[clientid]  = result
					kv.unlock("read state machine")

				}else {
					kv.unlock("read state machine")
					continue
				}

				msgindex := msg.CommandIndex

				kv.lock("save snap")
				kv.savesnapshot(msgindex)
				kv.unlock("save snap")
			}
		}

	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	DPrintf("Make a kvserver:%d.", kv.me)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	// You may need initialization code here.
	kv.kvStore      = make(map[string]string)
	kv.applyHistory = make(map[int64]int64)
	kv.resultCache  = make(map[int64]ClerkResult)
	kv.stopCh       = make(chan struct{})
	kv.readPersist(kv.persister.ReadSnapshot())

	go kv.waitapply()

	return kv
}
