package raft

type InstallSnapshotArgs struct {
	// do not need to implement "chunk"
	// remove "offset" and "done"
	Term              int    // leader的任期
	LeaderId          int    //follower可以重定向客户端
	LastIncludedIndex int    // 快照替换了该索引之前且包括该索引的所有日志
	LastIncludedTerm  int    // lastIncludedIndex的任期
	Data              []byte // 快照分块从offset开始的字节流
	//Offset            int    //分块在快照文件中的字节偏移
	//done              bool   //如果这是最后一块分块，为true
}

type InstallSnapshotReply struct {
	Term int // follower的当前任期，leader可以用来更新自己
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

	reply.Term = rf.currentTerm

	//如果参数的term<接收者的currentTerm，立即回复
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
	}

	rf.ToState(StateFollower)

	// outdated snapshot
	//如果该 snapshot 的 lastIncludedIndex 小于等于本地的 commitIndex，那说明本地已经包含了该 snapshot 所有的数据信息，
	//尽管可能状态机还没有这个 snapshot 新，即 lastApplied 还没更新到 commitIndex，但是 applier 协程也一定尝试在 apply 了，
	//此时便没必要再去用 snapshot 更换状态机了。对于更新的 snapshot，这里通过异步的方式将其 push 到 applyCh 中。
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

//处理reply
func (rf *Raft) handleInstallSnapshotResponse(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if reply.Term > rf.currentTerm { //返回的任期比现在要靠后，转变成跟随者
		rf.currentTerm = reply.Term
		rf.ToState(StateFollower)
		rf.persist()
	} else {
		//日志index跑到了matchindex后面，更新下
		if rf.matchIndex[server] < args.LastIncludedIndex {
			rf.matchIndex[server] = args.LastIncludedIndex
		}
		//把nextIndex更新为matchIndex+1，下一个要发给follower的index是已经匹配的index+1
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
