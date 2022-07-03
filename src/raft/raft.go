package raft

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

//Make
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
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("[Make]<Node %v> restart, logs: %v", rf.me, rf.logs)
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

	// start applier goroutine to push committed logs into applyCh exactly once
	go rf.applier()

	return rf
}

//Start
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
	term, isLeader = rf.GetState()
	if isLeader {
		rf.mu.Lock()
		index = rf.getLastLog().Index + 1 //先加一，方便持久化
		rf.logs = append(rf.logs, LogEntry{Command: command, Term: term, Index: index})
		rf.persist()

		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		DPrintf("[Start] <Node %v> receives command: |%v| in term %v", rf.me, command, rf.currentTerm)
		rf.Broadcast(false)
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

//GetState return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.state == StateLeader
	return term, isleader
}

// Kill
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

//Me lock before use
func (rf *Raft) Me() int {
	return rf.me
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Term() int {
	return rf.currentTerm
}
