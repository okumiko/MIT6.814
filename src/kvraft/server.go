package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

type OperationContext struct {
	commandId    int
	LastResponse *CommandResponse
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied int // record the lastApplied to prevent stateMachine from rollback

	stateMachine KVStateMachine // KV stateMachine
	//考虑这样一个场景，客户端向服务端提交了一条日志，服务端将其在 raft 组中进行了同步并成功 commit，接着在 apply 后返回给客户端执行结果。
	//然而不幸的是，该 rpc 在传输中发生了丢失，客户端并没有收到写入成功的回复。
	//因此，客户端只能进行重试直到明确地写入成功或失败为止，这就可能会导致相同地命令被执行多次，从而违背线性一致性。
	lastOperations map[int64]OperationContext    // 通过记录与clientId相对应的最后一个commandId和响应来确定日志是否重复
	notifyChans    map[int]chan *CommandResponse // notify client goroutine by applier goroutine to response
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) Put(args *PutArgs, reply *PutReply) {
	// Your code here.
}
func (kv *KVServer) Append(args *AppendArgs, reply *AppendReply) {
	// Your code here.
}

func (kv *KVServer) isDuplicateRequest(clientId int64, commandId int) bool {
	return kv.lastOperations[clientId].commandId == commandId
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
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	kv.stateMachine = NewMemoryKV()
	kv.notifyChans = make(map[int]chan *CommandResponse)
	kv.lastOperations = make(map[int64]OperationContext)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	go kv.applier()
	return kv
}
