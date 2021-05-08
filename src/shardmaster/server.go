package shardmaster


import (
	"../raft"
	"fmt"
	"log"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"


const WaitCmdTimeOut = time.Millisecond * 500
const MaxLockTime = time.Millisecond * 10 // debug

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	stopCh  chan struct{}
	// Your data here.

	configs []Config // indexed by config num


	lastApplies map[int64]int64 // last apply put/append msg
	replyCache  map[int64]cmdReply

	//Debug
	DebugLog  bool
	lockStart time.Time // debug 用，找出长时间 lock
	lockEnd   time.Time
	lockName  string
}


type Op struct {
	// Your data here.
	Args     interface{}
	Method   string
	ClientId int64
	MsgId    int64
}

type cmdReply struct {
	Err         Err
	Config      Config
	WrongLeader bool
}

func (sm *ShardMaster) isNewmsg(clientId int64, id msgId) bool {
	if val, ok := sm.lastApplies[clientId]; ok {
		return msgId(val) < id
	}
	return true
}

func (sm *ShardMaster) isRepeated(clientId int64, id int64) bool {
	if val, ok := sm.lastApplies[clientId]; ok {
		if val > id {
			panic("Old cmd.")
		}
		return val == id
	}
	return false
}

func (sm *ShardMaster) getConfigByIndex(idx int) Config {
	if idx < 0 || idx >= len(sm.configs) {
		return sm.configs[len(sm.configs)-1].Copy()
	} else {
		return sm.configs[idx].Copy()
	}
}

func (sm *ShardMaster) lock(m string) {
	sm.mu.Lock()
	sm.lockStart = time.Now()
	sm.lockName = m
}

func (sm *ShardMaster) unlock(m string) {
	sm.lockEnd = time.Now()
	duration := sm.lockEnd.Sub(sm.lockStart)
	sm.lockName = ""
	sm.mu.Unlock()
	if duration > MaxLockTime {
		sm.log(fmt.Sprintf("lock too long:%s:%s\n", m, duration))
	}
}

func (sm *ShardMaster) log(m string) {
	if sm.DebugLog {
		log.Printf("shardmaster me: %d, configs:%+v, log:%s", sm.me, sm.configs, m)
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	res := sm.runcmd("Join", args.ClientId ,args.MsgId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	res := sm.runcmd("Leave", args.ClientId ,args.MsgId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	res := sm.runcmd("Move", args.ClientId ,args.MsgId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.


	res := sm.runcmd("Query", args.ClientId ,args.MsgId, *args)
	reply.Err, reply.WrongLeader, reply.Config = res.Err, res.WrongLeader, res.Config
}

func (sm *ShardMaster) runcmd(method string, clientid int64, msgid int64, args interface{})(res cmdReply){
	_,isLeader := sm.rf.GetState()

	if !isLeader {
		res.Err = WrongLeader
		res.WrongLeader = true
		return
	}

	op := Op{
		MsgId:    msgid,
		Args:     args,
		Method:   method,
		ClientId: clientid,
	}
	return sm.waitCmd(op)
}

func (sm *ShardMaster) waitCmd(op Op) (res cmdReply){

	sm.lock("waitcmd1")
	if sm.isRepeated(op.ClientId, op.MsgId){
		res = sm.replyCache[op.ClientId]
		sm.unlock("waitcmd1")
		return
	}
	sm.unlock("waitcmd1")

	index, term, isLeader := sm.rf.Start(op)

	sm.log(fmt.Sprintf("start cmd: index:%d, term:%d, op:%+v", index, term, op))

	if !isLeader{
		res.Err = WrongLeader
		res.WrongLeader = true
		return
	}

	for i:=0; i < 50 ; i++{
		sm.lock("waitcmd2")
		if sm.isRepeated(op.ClientId, op.MsgId){
			res = sm.replyCache[op.ClientId]
			sm.unlock("waitcmd1")
			return
		}
		sm.unlock("waitcmd2")
		time.Sleep(WaitCmdTimeOut / 50)
	}

	res.Err = Timeout
	res.WrongLeader = true
	return
}

func (sm* ShardMaster) leave(args LeaveArgs){

}

func (sm* ShardMaster) join(args JoinArgs){

}

func (sm* ShardMaster) move(args MoveArgs){
	config := sm.getConfigByIndex(-1)
	config.Num += 1
	config.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, config)
}

func (sm* ShardMaster) query(args QueryArgs, res *cmdReply){
	res.Config = sm.getConfigByIndex(args.Num)

}

func (sm* ShardMaster) applycmd(){
	for {
		select {
		case <-sm.stopCh:
			sm.log("stop ch get")
			return
		case msg := <- sm.applyCh:
			if !msg.CommandValid {
				continue
			}
			sm.log(fmt.Sprintf("get msg:%+v", msg))
			op := msg.Command.(Op)

			sm.lock("apply cmd.")

			isrepeated := sm.isRepeated(op.ClientId, op.MsgId)
			if !isrepeated {
				res := cmdReply{
					Err: OK,
					WrongLeader: false,
				}

				switch op.Method {
				case "Query":
					sm.query(op.Args.(QueryArgs), &res)
				case "Leave":
					sm.leave(op.Args.(LeaveArgs))
				case "Join":
					sm.join(op.Args.(JoinArgs))
				case "Move":
					sm.move(op.Args.(MoveArgs))
				default:
					panic("Unknown Method")
				}

				sm.lastApplies[op.ClientId] = op.MsgId
				sm.replyCache[op.ClientId]  = res

			}
			sm.unlock("apply cmd.")
		}

	}



}




//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	close(sm.stopCh)
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.replyCache  = make(map[int64]cmdReply)
	sm.stopCh      = make(chan struct{})
	sm.lastApplies = make(map[int64]int64)

	return sm
}
