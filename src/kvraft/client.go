package kvraft

import (
	"../labrpc"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers  []*labrpc.ClientEnd
	// You will have to modify this struct.
	LeaderId    int
	ClientId	int64
	SeqcmdID 	int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) seqid() (int64){
	return atomic.AddInt64(&ck.SeqcmdID,1)
}



func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.LeaderId = 0
	ck.ClientId = nrand()
	ck.SeqcmdID = 0
	DPrintf("Make a clerk:%d.............",ck.ClientId)

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) (value string) {
	// You will have to modify this function.
	args := GetArgs{
		Key: key,
		ClientID: ck.ClientId,
		SeqcmdID: ck.seqid(),
	}

	i := ck.LeaderId

	for {
		reply := GetReply{}
		DPrintf("Clerk%d Get %v.seqid%d",args.ClientID,key,args.SeqcmdID)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply) //简单的retry？
		if ok {
			DPrintf("reply seqid:%d Get:%v", args.SeqcmdID, reply)
			switch reply.Err {
			case OK:
				ck.LeaderId = i
				return reply.Value
			case ErrWrongLeader:
				DPrintf("wrongLeader:%d", i)
				time.Sleep(ChangeLeaderTime)
				i = (i + 1) % len(ck.servers)
			case ErrNoKey:
				ck.LeaderId = i
				return ""
			case TimeOut:
				time.Sleep(ChangeLeaderTime)
				i = (i + 1) % len(ck.servers)
				continue
			default:
			}

		}else {
			time.Sleep(ChangeLeaderTime)
			i = (i + 1) % len(ck.servers)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientID: ck.ClientId,
		SeqcmdID: ck.seqid(),
	}

	i := ck.LeaderId

	for {
		reply := PutAppendReply{}
		DPrintf("Clerk%d put append %v.seqid%d",ck.ClientId,key,args.SeqcmdID)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply) //简单的retry？
		if ok {
			DPrintf("reply seqid:%d PutAppend:%v", args.SeqcmdID, reply)
			switch reply.Err {
			case OK:
				ck.LeaderId = i
				return
			case ErrWrongLeader:
				time.Sleep(ChangeLeaderTime)
				i = (i + 1) % len(ck.servers)
			case ErrNoKey:
				ck.LeaderId = i
				return
			case TimeOut:
				time.Sleep(ChangeLeaderTime)
				i = (i + 1) % len(ck.servers)
				continue
			default:
			}
		}else {
			i = (i + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
