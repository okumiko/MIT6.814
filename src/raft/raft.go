package raft

import (
	"6.824/labrpc"
	"sync"
	"time"
)

// rf = Make(...) 创建一个新的 Raft 服务器。
// rf.Start(command interface{}) (index, term, isleader) 在新的日志条目上启动协议
// rf.GetState() (term, isLeader) 询问 Raft 的当前任期，以及它是否认为自己是领导者
// ApplyMsg 每次向日志提交新条目时，每个 Raft 对等点都应向同一服务器中的服务（或测试者）发送 ApplyMsg。

import (
	"sync/atomic"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//raft节点
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//所有服务器上的持久化状态，在回复RPC之前更新持久化存储
	//term的主要作用是用于识别出过时信息。比如网络分区时，某一分区的server的term滞后，分区恢复后就能根据term值识别出过期的server，过期的server也可以根据收到的较大的term更新自己的term。
	currentTerm int        //服务器知道的最近任期，当服务器启动时初始化为0
	votedFor    int        //当前任期中，该服务器给投过票的candidateId，如果没有则为null
	logs        []LogEntry //日志条目；每一条包含了状态机指令以及该条目被leader收到时的任期号
	// 第一个条目是一个虚拟条目，其中包含 LastSnapshotTerm、LastSnapshotIndex 和 nil 命令

	//所有服务器上的易失性状态
	commitIndex int //已知被提交的最高日志条目索引号，一开始是0，单调递增
	lastApplied int //应用到状态机的最高日志条目索引号，一开始为0，单调递增

	//leader上的易失性状态，在选举之后重新初始化
	nextIndex  []int //针对所有的服务器，leader将发送给该follower的下一个日志条目的索引(初始化为leader的最高索引号+1)
	matchIndex []int //针对所有的服务器，目前已知的同步给这个节点的最高log的index。，初始化为0，单调递增

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond
	replicatorCond []*sync.Cond

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	state StateType
}

// 服务或测试者想要创建一个 Raft 服务器。
//所有 Raft 服务器（包括这个）的端口都在 peers[] 中。
//此服务器的端口是 peers[me]。
//所有服务器的 peers[] 数组都具有相同的顺序。
//persister 是此服务器保存其持久状态的地方，并且最初还保存最近保存的状态（如果有）。
//applyCh 是测试人员或服务期望 Raft 发送 ApplyMsg 消息的通道。
//  Make() 必须快速返回，因此它应该为任何长时间运行的工作启动 goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
		state:          StateFollower,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]LogEntry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// if there is no need to replicate entries for this peer, just release CPU and wait other goroutine's signal if service adds new Command
		// if this peer needs replicating entries, this goroutine will call replicateOneRound(peer) multiple times until this peer catches up, and then wait
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait() //goroutine休眠
		}
		// maybe a pipeline mechanism is better to trade-off the memory usage and catch up time
		rf.replicateOneRound(peer) //replicate协程负责发送心跳
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	//leader本地的最高日志比已知要复制的最高日志index高，说明要增加要复制的日志
	return rf.state == StateLeader && rf.matchIndex[peer] < rf.getLastLog().Index
}

//ticker 协程会定期收到两个 timer 的到期事件
//1. 如果是 election timer 到期，则发起一轮选举
//2. 如果是 heartbeat timer 到期且节点是 leader，则发起一轮心跳
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C: //选举定时器到时，自己变为候选者竞选
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				rf.ToState(StateCandidate) //转变为候选人
			}()
		case <-rf.heartbeatTimer.C:
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state == StateLeader { //在选举之前：发送空的AppendEntries RPC(心跳)给所有的服务器，一段时间后重复发送来防止选举超时的发生
					rf.BroadcastHeartbeat(true)                             //广播心跳
					resetTimer(rf.heartbeatTimer, StableHeartbeatTimeout()) //重置定时器时间
				}
			}()
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.state == StateLeader
	return term, isleader
}

// 将 Raft 的持久状态保存到稳定存储中，
// 崩溃后可以在其中检索它并重新启动。
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// 一个服务想要切换到快照。 只有在 Raft 没有更新的信息时才这样做，因为它在 applyCh 上传达了快照。
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	// outdated snapshot
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logs = make([]LogEntry, 1)
	} else {
		rf.logs = shrinkEntriesArray(rf.logs[lastIncludedIndex-rf.getFirstLog().Index:])
		rf.logs[0].Command = nil
	}
	// update dummy entry with lastIncludedTerm and lastIncludedIndex
	rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//使用 Raft 的服务（例如一个 k/v 服务器）想要就下一个要附加到 Raft 日志的命令开始协议。
//如果此服务器不是领导者，则返回 false。 否则启动协议并立即返回。
//无法保证此命令将永远提交到 Raft 日志，因为领导者可能会失败或失去选举。
//即使 Raft 实例被杀死，这个函数也应该优雅地返回。
//第一个返回值是该命令在提交时将出现的索引。 第二个返回值是当前术语。 如果此服务器认为它是领导者，则第三个返回值为 true。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// need sending at once to maintain leadership
			go rf.replicateOneRound(peer)
		} else {
			// just signal replicator goroutine to send entries in batch
			rf.replicatorCond[peer].Signal()
		}
	}
}
