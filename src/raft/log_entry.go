package raft

//日志条目
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type AppendEntriesArgs struct {
	Term     int // leader的任期号
	LeaderId int // 用来让follower把客户端请求定向到leader

	PrevLogIndex int         // 紧接新条目之前的日志条目索引(当前最大的日志条目索引)
	PrevLogTerm  int         // prevLogIndex的任期
	LogEntries   []*LogEntry // 储存的日志条目(如果某条目是空的，它就是心跳；为了提高效率可能会发出不止一条日志)
	LeaderCommit int         // leader的commitIndex
}

type AppendEntriesReply struct {
	Term    int  // 当前任期，用来让leader更新自己
	Success bool // 如果follower包含的日志匹配参数汇总的prevLogIndex和prevLogTerm，返回true

	// OPTIMIZE: see thesis section 5.3
	ConflictTerm  int // 2C
	ConflictIndex int // 2C
}

//leader收到客户端的命令后，封装成一个log entry，append到本地log中，然后向所有的follower发送AppendEntries RPC。当收到多数follower的响应时，leader认为该log entry已提交，然后可以在本地状态机上执行该命令，并返回结果给客户端，同时通知各个follower该log entry已提交，follower收到该通知后就可以将命令送入状态机执行。
//从上述描述可以总结出一点，log entry已提交就是指该log entry在大多数server上都有了备份，且大多数server知晓这一点。
//对于那些还没向leader发送响应的follower，leader会不断向它们发送AppendEntries RPC，直到它们成功响应。
//log一致性特性：
//如果两个server上的log entry有相同的index和term，则该index中存的命令一定相同；
//小于该index的所有log entry 一定相同。
//如果某一个log entry已被提交，则该log entry之前的所有log entry（index更小的log entry）均已被提交。
//prevLogIndex：leader的本地log中最新的log entry之前的log entry（因为leader是将客户端命令封装成log entry并append到本地log之后才开始复制日志的，所以才会说“之前”）的index
//prevLogTerm：leader的本地log中最新的log entry之前的log entry的term
//entries[]：将要复制到follower的log entries，可以不止一条
//当leader接收了客户端发起的一个新的命令，并将命令封装成log entry写入了本地log后，他需要将该log entry复制到所有的follower上，这时follower不仅仅是简单地将log entry写入到本地log即可，还需要在写入之前检查所有已有的log entry是否与leader中的一致。follower将prevLogIndex和prevLogTerm这两个参数与自己本地最新的log entry的index和term进行对比，如果相同，可以认为自己的本地log与leader是一致的，然后就可以将新的log entry append到本地log中；如果对比发现不相同，则拒绝append，并返回false。
//上述提到的这种检查其实是一种递归检查。这一次检查发现prevLogIndex和prevLogTerm与本地最新的log entry的index和term是匹配的，说明上一次检查时相应的prevLogIndex和prevLogTerm也是匹配的，一直递归到log为空，append第一条log entry时，prevLogIndex和prevLogTerm都为0，也是匹配的。所以每次检查时如果prevLogIndex和prevLogTerm与本地最新的log entry匹配，则之前的所有的log entry也是一致的。
//那么follower在检查完之后，如果本地最新的log entry是一致的，则本次将log entry append到本地log中之后，整个log与leader上的log仍然是一致的。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // execute before rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), request, response)

	//如果参数term小于接收者的currentTerm，返回false,过期消息，拒绝复制
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	//消息任期靠后，一定要成为跟随者
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	rf.ChangeState(StateFollower)

	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term, reply.Success = 0, false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, request, request.LeaderId, request.PrevLogIndex, rf.getFirstLog().Index)
		return
	}

	//本地log里没有和preLogTerm以及preLogIndex相匹配的log，说明领导者那里的log除了这次发的还有没同步的，需要把之前的也同步过来
	if !rf.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		reply.Term, reply.Success = rf.currentTerm, false //拒绝接受，返回自己的term
		//lastIndex := rf.getLastLog().Index
		//if lastIndex < args.PrevLogIndex {
		//	reply.ConflictTerm, reply.ConflictIndex = -1, lastIndex+1
		//} else {
		//	firstIndex := rf.getFirstLog().Index
		//	reply.ConflictTerm = rf.logs[args.PrevLogIndex-firstIndex].Term
		//	index := args.PrevLogIndex - 1
		//	for index >= firstIndex && rf.logs[index-firstIndex].Term == reply.ConflictTerm {
		//		index--
		//	}
		//	reply.ConflictIndex = index
		//}
		return
	}

	//本地log:|40-1|41-2|42-3|43-4|44-5|
	//请求log:|43-5|44-6|45-7|46-8|47-9|
	//preLogIndex=42,前面已经判断了match
	//因为绝对相信领导者，找到index=43的term不一样，发生冲突。本地log后面的都要删除，请求log 45后面的都要写入。
	firstIndex := rf.getFirstLog().Index
	for index, entry := range args.LogEntries {
		if entry.Index-firstIndex >= int(len(rf.logs)) || rf.logs[entry.Index-firstIndex].Term != entry.Term { //args里遍历剩下的日志本地没有，直接同意更新 或者 遇到了任期不同的日志，舍弃后面的
			rf.logs = shrankEntriesArray(append(rf.logs[:entry.Index-firstIndex], args.LogEntries[index:]...))
			break
		}
	}

	rf.advanceCommitIndexForFollower(request.LeaderCommit)

	reply.Term, args.Success = rf.currentTerm, true
}

//使用前上锁
func (rf *Raft) matchLog(PrevLogTerm, PrevLogIndex int) bool {
	relativeIndex := PrevLogIndex - rf.getFirstLog().Index //
	return rf.logs[relativeIndex].Term == PrevLogTerm
}

//使用前上锁
func (rf *Raft) getLastLog() *LogEntry {
	lastSnapshotIndex := rf.logs[0].Index
	n := len(rf.logs)
	return &LogEntry{
		Term:  rf.logs[n-1].Term,
		Index: lastSnapshotIndex + int(n) - 1,
	}
}

func (rf *Raft) getFirstLog() *LogEntry {

}

//本地logs属于长久的切片，在经过合并后会造成cap比len长
//切片里存放的是指针对象，那么下面删除末尾的元素后，被删除的元素依然被切片底层数组引用，从而导致不能及时被自动垃圾回收器回收
//采用复制一份大小合适的
func shrankEntriesArray(logs []*LogEntry) []*LogEntry {
	shrankLogs := make([]*LogEntry, len(logs))
	copy(shrankLogs, logs)
	logs = nil
	return shrankLogs
}
