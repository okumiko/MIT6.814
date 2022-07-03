package raft

//Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//删除掉对应已经被压缩的 raft log 即可，index是快照里最后一个log的index
//本节点生成快照，输入：新的快照index，以及快照logs
func (rf *Raft) Snapshot(newSnapshotIndex int, snapshot []byte) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Snapshot]<Node %v|Term %v>", rf.me, rf.currentTerm)
	snapshotIndex := rf.getFirstLog().Index
	if newSnapshotIndex <= snapshotIndex { //输入的index太早，返回
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, newSnapshotIndex, snapshotIndex, rf.currentTerm)
		return
	}
	//删除两个index间的日志
	rf.logs = shrinkEntriesArray(rf.logs[newSnapshotIndex-snapshotIndex:])
	rf.logs[0].Command = nil                                      //logs[0]保存快照index和term,现在log[0]就是快照里最新的一个log，把log清空，index和term就是快照里最新log的index和term，机智
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot) //保存节点状态和快照
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), newSnapshotIndex, snapshotIndex)
}

type InstallSnapshotArgs struct {
	// do not need to implement "chunk"
	// remove "offset" and "done"
	Term              int    // leader的任期
	LeaderId          int    //follower可以重定向客户端
	LastIncludedIndex int    // 快照替换了该索引之前且包括该索引的所有日志
	LastIncludedTerm  int    // lastIncludedIndex的任期
	Data              []byte // 快照分块从offset开始的字节流
}

type InstallSnapshotReply struct {
	Term int // follower的当前任期，leader可以用来更新自己
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//InstallSnapshot RPC：通过leader调用，发送快照的分块给follower。leader将分块按顺序发送。
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v",
		rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	reply.Term = rf.currentTerm

	//如果参数的term<接收者的currentTerm，立即回复
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
	}

	rf.becomeFollower()

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
		rf.becomeFollower()
		{
			rf.currentTerm = reply.Term
			rf.persist()
		}
	} else {
		//日志index跑到了matchIndex后面，更新下
		if rf.matchIndex[server] < args.LastIncludedIndex {
			rf.matchIndex[server] = args.LastIncludedIndex
		}
		//把nextIndex更新为matchIndex+1，下一个要发给follower的index是已经匹配的index+1
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	}
}

//CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//对于服务上层触发的 CondInstallSnapshot，与上面类似，如果 snapshot 没有更新的话就没有必要去换，否则就接受对应的 snapshot 并处理对应状态的变更。
//注意，这里不需要判断 lastIncludeIndex 和 lastIncludeTerm 是否匹配，因为 follower 对于 leader 发来的更新的 snapshot 是无条件服从的。
//lastInclude是指快照里最新的log
//跟随着收到InstallSnapshot RPC后，在通过向channel发送快照消息异步应用到状态机
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	// 过期的快照
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex > rf.getLastLog().Index { //用快照替换
		rf.logs = make([]LogEntry, 1)
	} else { //快照index之前的log可以删除了，已经持久化了
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
