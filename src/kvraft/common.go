package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	TimeOut        = "Timeout"
)

const (
	Putstr      = "Put"
	Appendstr   = "Append"
	Getstr      = "Get"

)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID	int64
	SeqcmdID	int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key         string
	ClientID	int64
	SeqcmdID	int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
