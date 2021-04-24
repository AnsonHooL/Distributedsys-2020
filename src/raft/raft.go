package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labgob"
	"bytes"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//Tester要求每秒心跳包少于10次
//选举时间超时应该设置为 >> 心跳包时间间隔, 这里选了2倍心跳时间
const(
	HeartBeatTime      time.Duration = 150 * time.Millisecond
	ElectionTimeOut    time.Duration = 100 * time.Millisecond * 3 //这个调小一点整体速度会上去
	MaxLockTime        time.Duration = 10 * time.Millisecond
	ApplychTimeout     time.Duration = 100 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type PeerStatus int

const (
	Leader      PeerStatus = 0
	Follower    PeerStatus = 1
	Candidate   PeerStatus = 2
)

type logEntry struct {
	Term    int
	Command interface{}
	Index   int //For Debug
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	peerstatus      PeerStatus //节点身份

	///持久化的状态
	currentTerm     int //节点当前任期
	votedFor		int //节点所投票,一般转成follower时需要重置
	logArray        []logEntry //日志

	//所有节点都有的状态
	commitIndex     int //已提交日志，Leader通过MatchIndex决定什么时候提交入职
	lastApplied		int //已执行日志，作用几乎等同commitIndex
	electTimeout    time.Time //超时，发起选举时间,然后我定时睡眠，检查是否超时了
	heartTimeout    []time.Time

	applyCh         chan ApplyMsg //回答日志已经提交，applyCh

	//Leader才有的状态
	nextIndex       []int //Leader下一次发送日志的索引号,每次当选都要重设
	matchIndex		[]int //其他节点，日志匹配的索引号，一般 = nextIndex - 1

	///我添加的状态
	tickets         int //选举得到的票数
	EelectRPC       int //记录RPC数量
	HeartRPC		int

	//debug
	lockStart	    time.Time
	lockEnd			time.Time
	lockName		string
}

func (rf *Raft) lock(m string) {
	rf.mu.Lock()
	rf.lockStart = time.Now()
	rf.lockName = m
}

func (rf *Raft) unlock(m string) {
	rf.lockEnd = time.Now()
	duration := rf.lockEnd.Sub(rf.lockStart)
	if rf.lockName != "" && duration > MaxLockTime {
		DPrintf("lock too long:%s:%s:iskill:%v", m, duration, rf.killed())
	}
	rf.mu.Unlock()
}

func Initrand() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	rand.Seed(time.Now().Unix() + 100)
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).

	rf.lock("Get State Lock")
	defer rf.unlock("Get State Unlock")

	term = rf.currentTerm
	if rf.peerstatus == Leader{
		isleader = true
	}else {
		isleader = false
	}

	return term, isleader
}

//重置选举时间
func randElectionTimeout() time.Time {
	r := time.Duration(rand.Int63()) % ElectionTimeOut
	return time.Now().Add(ElectionTimeOut + r)
}

//重置心跳时间
func randHeartBeatTimeout() time.Time {
	//r := time.Duration(rand.Int63())  % HeartBeatTime
	//return time.Now().Add(HeartBeatTime + r)
	return time.Now().Add(HeartBeatTime ) //【坑】用下面这条，心跳，然后就会出现问题，想了一下，是不是上面的rand减少了锁的争用啊，好像也不对，确实没问题，是因为我一开始多发RPC的bug
}

//返回me的最后一条日志的trem、index
func (rf *Raft) lastLogTermIndex()(term,index int){
	term  = rf.logArray[len(rf.logArray) - 1].Term
	index = len(rf.logArray) - 1
	return
}

//切换结点身份
func (rf *Raft) switchStatus_nolock(status PeerStatus){
	switch status {
	case Follower:
		rf.peerstatus = Follower
	case Candidate:
		rf.peerstatus = Candidate
	case Leader:
		rf.peerstatus = Leader
		_, lastLogIndex := rf.lastLogTermIndex()

		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = lastLogIndex



	}
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	//e.Encode(rf.commitIndex)
	e.Encode(rf.logArray)
	data := w.Bytes()
	return data
}

//没加锁，一定要在锁范围内调用，并且要在数据更新后才能调用，数据一改就要更新，不能等，不能等到发送RPC才持久化，这样
func (rf *Raft) persist() {
	data := rf.getPersistData()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var voteFor int
	var logs []logEntry
	//var commitIndex int

	if  d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		//d.Decode(&commitIndex) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("rf read persist err")
	} else {
		rf.currentTerm = term
		rf.votedFor = voteFor
		//rf.commitIndex = commitIndex
		rf.logArray = logs
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term           int //当前节点任期
	CandidateID    int //候选节点ID

	//2B
	LastLogIndex   int //候选人的最后一条日志索引，日志比我长
	LastLogTerm    int //候选人的最后一条日志任期，日志比我新
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term	       int   //对方节点的任期
	VoteGranted    bool  //
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock("Request Vote")
	defer rf.unlock("Request Vote")


	DPrintf("%d request vote to %d",args.CandidateID, rf.me)


	if rf.currentTerm > args.Term{ //过期RPC
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("vote:%v %d-->%d",reply.VoteGranted, rf.me, args.CandidateID)
		return
	}else if rf.currentTerm == args.Term{
		if rf.peerstatus == Leader || rf.peerstatus == Candidate{
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			DPrintf("vote:%v %d-->%d",reply.VoteGranted, rf.me, args.CandidateID)
			return
		}else { //follower
			if rf.votedFor == args.CandidateID {  //之前投的票RPC有可能丢失 或者 在这一轮没有投票
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				rf.electTimeout = randElectionTimeout() ///重新定时,注意这里一定要正确的重置，否则选举很容易活锁【坑坑坑】
				DPrintf("vote:%v %d-->%d",reply.VoteGranted, rf.me, args.CandidateID)
				return
			} else if rf.votedFor == -1 { //还没投票呢
				mylastlogterm,mylastlogindex := rf.lastLogTermIndex()
				if args.LastLogTerm > mylastlogterm || (args.LastLogTerm == mylastlogterm && args.LastLogIndex >= mylastlogindex) { //许安出最新的Leader
					reply.Term = rf.currentTerm
					reply.VoteGranted = true
					rf.electTimeout = randElectionTimeout() ///重新定时
					rf.votedFor = args.CandidateID //这句我一开始居然忘记写了。。。差点出大问题，坑啊
					DPrintf("vote:%v %d-->%d",reply.VoteGranted, rf.me, args.CandidateID)
					rf.persist()
					return
				}
			}
			 //票投给了其他人
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			DPrintf("vote:%v %d-->%d",reply.VoteGranted, rf.me, args.CandidateID)
			return
		}
	}else { //RPC的任期比当前节点大,因此进行投票给他，不考虑发送日志情况下,现在要考虑了！！！！【坑，忘记改了lab2b】
		rf.currentTerm = args.Term
		rf.switchStatus_nolock(Follower)

		reply.Term = rf.currentTerm

		mylastlogterm,mylastlogindex := rf.lastLogTermIndex()
		if args.LastLogTerm > mylastlogterm || (args.LastLogTerm == mylastlogterm && args.LastLogIndex >= mylastlogindex) {
			rf.votedFor = args.CandidateID
			rf.electTimeout = randElectionTimeout() ///重新定时
			reply.VoteGranted = true
		}else {
			rf.votedFor = -1
			reply.VoteGranted = false
		}

		rf.persist()
		return
	}

	DPrintf("NO rule for vote here")
	os.Exit(-1)
	// persisitent(2C)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//


//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}

func (rf *Raft) sendRequestVoteToPeer(peer *labrpc.ClientEnd, electerm, lastlogindex, lastlogterm int){
	//electionOneTime := ElectionTimeOut / 20  //发起一次RPC的时间不应该超过这个时间
	for i := 0; i < 1; i++ {
		args := RequestVoteArgs{
			Term: electerm,
			CandidateID: rf.me,
			LastLogIndex: lastlogindex,
			LastLogTerm: lastlogterm,
		}
		reply := RequestVoteReply{
			VoteGranted: false,
		}

		//rf.lock("")
		//if rf.peerstatus != Candidate{
		//	rf.unlock("")
		//	return
		//}
		//rf.unlock("")

		ok := peer.Call("Raft.RequestVote", &args, &reply)

		rf.lock("")
		rf.EelectRPC++
		DPrintf("server:%d,Elect time:%d",rf.me,rf.EelectRPC)
		rf.unlock("")


		if ok == true{
			//DPrintf("Request RPC fail success")
			if reply.VoteGranted == false{
				rf.lock("send request vote and handle")
				if reply.Term > rf.currentTerm{
					rf.currentTerm = reply.Term
					rf.switchStatus_nolock(Follower)
					rf.votedFor = -1
					rf.electTimeout = randElectionTimeout()
					rf.persist()
				}
				rf.unlock("send request vote and handle")
				return
			}else {
				rf.lock("send request vote and handle")
				if args.Term != rf.currentTerm || rf.peerstatus != Candidate{
					rf.unlock("send request vote and handle")
					return
				}else {
					rf.tickets = rf.tickets + 1
					if rf.tickets > len(rf.peers) / 2 {
						rf.switchStatus_nolock(Leader)
						DPrintf("Server:%d  become a Leader in Term: %d",rf.me, rf.currentTerm)
						for i,_ := range rf.heartTimeout{
							rf.heartTimeout[i] = time.Now()
						}

						rf.unlock("send request vote and handle")
						return
					}
					rf.unlock("send request vote and handle")
					return
				}
			}
		}else {
			//DPrintf("Request RPC fail")
			//time.Sleep(electionOneTime) 只投一次票，就不睡了
		}
	}


}

//生成每个心跳包的args，计算出应该发生多少log给peer
func (rf* Raft) getAppendEntryArgs(peer int)(args AppendEntriesArgs){
	args.Term = rf.currentTerm
	args.LeaderId = rf.me

	_, lastindex := rf.lastLogTermIndex()
	nextindex := rf.nextIndex[peer]

	//如果最后一个log index 大于等于改发给peer的nextindex，那么就应该发送日志
	if lastindex >= nextindex{
		args.Log = rf.logArray[nextindex: ]
	}else {
		args.Log = nil
	}

	args.PrevLogTerm  = rf.logArray[nextindex - 1].Term
	args.PrevLogIndex = nextindex - 1
	args.LdcommitIDX  = rf.commitIndex
	//DPrintf("args of peer:%d,nextindex:%d,previndex:%d,prevterm:%d",peer,nextindex,nextindex-1,rf.logArray[nextindex-1])
	return
}




func (rf *Raft) sendHeartBeattopeer(peerindex int, peer *labrpc.ClientEnd){
	rf.lock("sendHeartBeattopeer")
	//这里不能用defer unlock，会被阻塞卡死

	args := rf.getAppendEntryArgs(peerindex)

	reply := AppendEntriesReply{
		Success: false,
	}


	rf.unlock("sendHeartBeattopeer")

	//【坑坑坑】，必须检查自己是否还是Leader才能发起RPC，因为可能在发送前，自己的Term已经发送了变化，然后还发送就会出事
	_, isLeader := rf.GetState()
	if !isLeader{
		return
	}

	ok := peer.Call("Raft.AppendEntries", &args, &reply)


	rf.lock("")

	rf.HeartRPC++
	DPrintf("server:%d,Append time:%d",rf.me,rf.HeartRPC)
	rf.unlock("")


	if ok == true{
		rf.lock("send Heart Beat topeer and handle")
		defer rf.unlock("send Heart Beat topeer and handle")

		if reply.Term > rf.currentTerm{ //Leader需要主动放弃位置
			rf.switchStatus_nolock(Follower)
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.electTimeout = randElectionTimeout()

			rf.persist()
		}else if args.Term == rf.currentTerm && rf.peerstatus == Leader{ //这里需要判断返回的reply，如果是同期的才有用，如果me已经不是leader就没用了【坑点！！！！！】
			if reply.Success { //如日志匹配成功，更新结点的信息
				//DPrintf("success heart")
				if rf.matchIndex[peerindex] < args.PrevLogIndex + len(args.Log){
					rf.matchIndex[peerindex] = args.PrevLogIndex + len(args.Log) // match index = prev index + 发送长度
					rf.nextIndex[peerindex] = rf.matchIndex[peerindex] + 1 //next index = match index + 1
				}

				oldcommitIDX := rf.commitIndex

				if len(args.Log) > 0 { //非心跳包，才需要更新commit index
					for comidx := rf.commitIndex + 1; comidx < len(rf.logArray) ; comidx++ {
						vote := 0
						for j,_  := range rf.peers{
							if rf.matchIndex[j] >= comidx{
								vote++
							}
						}
						if vote > len(rf.peers) / 2{ //过半才commit
							if rf.logArray[comidx].Term == rf.currentTerm{ //注意：只能确认自己任期产生的log
								rf.commitIndex = comidx
								DPrintf("Leader:%d commit log index:%d in term:%d",rf.me,comidx,rf.currentTerm)
							}
							///TODO：这里要将最新的commitIndex通知一下客户端applych
						}else {
							break
						}
					}
				}

				if oldcommitIDX != rf.commitIndex{
					rf.persist()
				}

			}else { //若日志匹配失败,现在还是慢速恢复
				//DPrintf("prevlogterm:%d,prevlogindex:%d",args.PrevLogTerm, args.PrevLogIndex)
				//DPrintf("%d : fail heart:%v",peerindex,reply.Success)
				//rf.nextIndex[peerindex] = args.PrevLogIndex //这里不能直接nextindex--，是不是我发多了啊RPC，是的,其实都行只要不多发RPC【好坑】
				rf.nextIndex[peerindex] = rf.FastNextIndex(&reply, &args)

				//rf.nextIndex[peerindex]--
				rf.heartTimeout[peerindex] = time.Now() //TestBackup2B,就靠这一行代码了，牛逼！【坑】，有了快速恢复，这条也是可加可不加
				if rf.nextIndex[peerindex] < 1 {
					//DPrintf("args:%v",args)
					DPrintf("Impossible Nextindex < 1.")
					os.Exit(-1)
				}
			}
		}
		return
	}else {
		//心跳包发送失败，客户端可能挂了，暂时不需要处理
	}
}



/**
发起一场选举，向每一位节点发送投票，为了不被失败的RPC阻塞，
所以每一个节点都开了一个协程去完成
*/

func (rf *Raft) startElection(){
	rf.lock("Start an Election")
	defer rf.unlock("Start an Election")


	//除了Leader，其他都会发起竞选
	if rf.peerstatus == Leader{
		return
	}else{
		rf.switchStatus_nolock(Candidate)
		DPrintf("server:%d old term:%d",rf.me,rf.currentTerm)
		rf.currentTerm = rf.currentTerm + 1
		DPrintf("server:%d new term:%d",rf.me,rf.currentTerm)
		DPrintf("server:%d start an election, term: %d", rf.me, rf.currentTerm)
		rf.votedFor = rf.me
		rf.tickets = 1

		rf.persist()

		for index, peer := range rf.peers{
			if index == rf.me{
				continue
			}else {
				lastlogterm, lastlogindex := rf.lastLogTermIndex()
				go rf.sendRequestVoteToPeer(peer, rf.currentTerm, lastlogindex, lastlogterm)
			}
		}
	}
}


func (rf *Raft) sendHeartBreak(peerindex int){
	rf.lock("send heart break")
	defer rf.unlock("send heart break")

	if rf.peerstatus != Leader {
		return
	}else {
			go rf.sendHeartBeattopeer(peerindex, rf.peers[peerindex])
		}
}



type AppendEntriesArgs struct {
	Term         int //Leader的任期
	LeaderId     int //follower redirect Leader


	PrevLogIndex int //上一条log的index
	PrevLogTerm  int //上一条log的term
	LdcommitIDX  int //Leader commit的log索引号
	Log			 []logEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool //这是检验AppendEntry包的prevlog是否和自己匹配上了

	//用于快速backup
	Xterm   int  //记录不匹配时，该点的Term或者为-1
	Xindex  int  //Term的第一条记录
	Xlen    int  //空洞长度
}


func (rf* Raft) FastNextIndex(reply *AppendEntriesReply, args *AppendEntriesArgs)(nextindex int){
	if reply.Xterm == -1 {
		nextindex = args.PrevLogIndex - reply.Xlen
	}else{
		if rf.logArray[reply.Xindex].Term == reply.Xterm{
			nextindex = reply.Xindex + 1
		}else {
			nextindex = reply.Xindex
		}
	}
	return
}

func (rf* Raft) Fastbackup(args* AppendEntriesArgs)(Xterm,Xindex,Xlen int){
	if args.PrevLogIndex >= len(rf.logArray){
		Xterm  = -1
		Xindex = -1
		Xlen = args.PrevLogIndex - len(rf.logArray)
	}else {
		Xterm  = rf.logArray[args.PrevLogIndex].Term
		Xindex = args.PrevLogIndex
		Xlen   = -1
		for {
			if rf.logArray[Xindex - 1].Term != Xterm{
				break
			}else {
				Xindex--
			}
		}
	}
	return
}


func (rf* Raft) HandleAppendLog(args* AppendEntriesArgs)(flag bool){

	//判断日志是否成功匹配上
	if  args.PrevLogIndex < len(rf.logArray) && rf.logArray[args.PrevLogIndex].Term == args.PrevLogTerm{
		flag = true

		if len(args.Log) > 0{

			if args.PrevLogIndex + len(args.Log) < len(rf.logArray) && args.Log[len(args.Log)-1].Term == rf.logArray[args.PrevLogIndex + len(args.Log)].Term{
				return //【坑】不能截断有效日志，因为RPC可能乱序到达
			}

			DPrintf("follower get a log success")
			rf.logArray = append(rf.logArray[0 : args.PrevLogIndex + 1], args.Log...) //如果和Leader匹配的话，需要截断后面的

			if args.LdcommitIDX >= len(rf.logArray) - 1{

				rf.commitIndex = len(rf.logArray) - 1

			}else{
				rf.commitIndex = args.LdcommitIDX
				///TODO：同样需要通知一下applych，更新一下applyid
			}
			DPrintf("server:%d term:%d . follower comIDX:%d, follower appid:%d. log len:%d",rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied,len(rf.logArray))
		}else {      //没收到新的log，心跳包，也要检查是否需要更新commitid【坑点】
			if args.LdcommitIDX >= len(rf.logArray) - 1{
				rf.commitIndex = len(rf.logArray) - 1
			}else{
				rf.commitIndex = args.LdcommitIDX
				///TODO：同样需要通知一下applych，更新一下applyid
			}
		}
	}else {
		DPrintf("follower get a log fail")
		DPrintf("Leader's lastlogindex:%d,Leader's lastloginterm:%d, myloglen:%d",args.PrevLogIndex,args.PrevLogTerm,len(rf.logArray))
		DPrintf("server:%d term:%d follower comIDX:%d, follower appid:%d",rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied)
		flag = false
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.lock("AppendEntries")
	defer rf.unlock("AppendEntryies")
	defer DPrintf("LabA, Heart breat! Term:%v,Leader:%d-->Follower:%d.Len of log:%d",args.Term, args.LeaderId, rf.me,len(args.Log))

	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term{ //过期RPC
		//reply.Success = true
		return
	}else if rf.currentTerm == args.Term{
		//if args.PrevLogIndex >= len(rf.logArray) || rf.logArray[args.PrevLogIndex].Term != args.PrevLogTerm{
		//	return
		//}else{
		//！！！！这里出大问题，可能是候选人收到了心跳包
		rf.switchStatus_nolock(Follower)
		rf.electTimeout = randElectionTimeout()
		reply.Success = rf.HandleAppendLog(args)

		if reply.Success == false{
			reply.Xterm, reply.Xindex, reply.Xlen = rf.Fastbackup(args)
		}
		//reply.Success = true
		//DPrintf("note:%d reply success:%v",rf.me,reply.Success)
		//DPrintf("lastlogindex:%d,lastloginterm:%d, myloglen:%d",args.PrevLogIndex,args.PrevLogTerm,len(rf.logArray))
		rf.persist()
		return
		//}
	}else { //RPC的任期比当前节点大,因此升级
		rf.switchStatus_nolock(Follower)
		rf.votedFor = args.LeaderId
		rf.currentTerm = args.Term
		rf.electTimeout = randElectionTimeout()

		reply.Success = rf.HandleAppendLog(args)
		if reply.Success == false{
			reply.Xterm, reply.Xindex, reply.Xlen = rf.Fastbackup(args)
		}
		//reply.Success = true
		//DPrintf("note:%d reply success:%v",rf.me,reply.Success)
		//DPrintf("lastlogindex:%d,lastloginterm:%d, myloglen:%d",args.PrevLogIndex,args.PrevLogTerm,len(rf.logArray))
		rf.persist()
		return
	}
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index, term int , isLeader bool) {
	rf.lock("Start get command")
	defer rf.unlock("Start get command")

	term = rf.currentTerm
	index = len(rf.logArray)
	isLeader = rf.peerstatus == Leader

	if !isLeader {
		return
	}else {
		rf.logArray = append(rf.logArray, logEntry{
			Command: command,
			Term: rf.currentTerm,
			Index: index,
		})
		rf.matchIndex[rf.me] = rf.matchIndex[rf.me] + 1
		DPrintf("note:%d,get a command.Term:%d,index:%d",rf.me,rf.currentTerm, index)

		for i,_ := range rf.peers {    //【坑】有命令来了应该马上就发起心跳包，也是一个大大加快共识的点，加上了速度快很多
			rf.heartTimeout[i] = time.Now()
		}

		rf.persist()
		return
	}
	// Your code here (2B).
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}




//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.switchStatus_nolock(Follower)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logArray = make([]logEntry, 1)
	rf.commitIndex = 0


	//2C
	rf.readPersist(persister.ReadRaftState())

	rf.electTimeout = randElectionTimeout() //初始化设定选举时间
	Initrand()
	//2B initial
	rf.applyCh = applyCh

	rf.lastApplied = 0

	rf.heartTimeout = make([]time.Time, len(peers))

	rf.logArray[0] = logEntry{
		Term: 0,
		Index: 0,
	}



	DPrintf("make raft........")
	///定时投票
	go func(){

		sleepforvote_Dura := ElectionTimeOut / 20 //每隔一段时间检查是否发起投票

		cout := 0

		for{
			rf.lock("read time")
			if time.Now().After(rf.electTimeout) {
				rf.electTimeout = randElectionTimeout() //成为了候选人也要重置竞选时间
				//DPrintf("server:%d,%dth :timeout",rf.me,cout)
				cout = cout + 1
				go rf.startElection()
				rf.unlock("read time")
			}else {
				rf.unlock("read time")
				time.Sleep(sleepforvote_Dura)
				if rf.killed(){
					return
				}
			}
		}

	}()

	///定时心跳
	for i, _ := range rf.peers {

		go func(peerindex int) {
			sleepheartDura := HeartBeatTime / 10
			for{
				_, isLeader := rf.GetState()

				rf.lock("read time")

				if time.Now().After(rf.heartTimeout[peerindex]) && isLeader && peerindex != rf.me{ //Leader才发送

					rf.heartTimeout[peerindex] = randHeartBeatTimeout()

					go rf.sendHeartBreak(peerindex)

					rf.unlock("read time")
				}else {
					rf.unlock("read time")
					time.Sleep(sleepheartDura)
					if rf.killed(){
						return
					}
				}
			}
		}(i)

	}


	go func() {
		sleepforapply := ApplychTimeout / 10
		for {

			rf.lock("read apply")
			lastapplyid := rf.lastApplied
			commitid    := rf.commitIndex
			rf.unlock("read apply")


			if lastapplyid < commitid {
				for ;lastapplyid < commitid; lastapplyid++{

					rf.lock("read data")
					//TODO：下面的数据只在这里被修改所以不用加锁，只是暂时不加锁
					msg := ApplyMsg{
						Command: rf.logArray[lastapplyid +1].Command, //这里out of range 了
						CommandValid: true,
						CommandIndex: lastapplyid + 1,
					}

					term := rf.logArray[lastapplyid+1].Term
					DPrintf("server:%d, send commnd, index: %d, term: %d. NOW is term:%d",rf.me,lastapplyid + 1,term,rf.currentTerm)

					rf.unlock("read data")

					KVPrintf("send a msg to KVserver")
					rf.applyCh <- msg

					rf.lock("read data")
					rf.lastApplied = lastapplyid + 1
					rf.unlock("read data")


				}
			}else {
				time.Sleep(sleepforapply)
				if rf.killed(){
					return
				}
			}
		}
	}()

	// initialize from state persisted before a crash

	return rf
}
