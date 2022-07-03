package raft

type (
	LogEntry struct {
		Command interface{}
		Term    int
		Index   int
	}
	AppendEntriesArgs struct {
		LeaderId int // 用来让follower把客户端请求定向到leader
		Term     int // leader的任期号

		PrevLogIndex      int        // 紧接新条目之前的日志条目索引(当前最大的日志条目索引)
		PrevLogTerm       int        // prevLogIndex的任期
		LogEntries        []LogEntry // 储存的日志条目(如果某条目是空的，它就是心跳；为了提高效率可能会发出不止一条日志)
		LeaderCommitIndex int        // leader的commitIndex
	}
	AppendEntriesReply struct {
		Term    int  // 当前任期，用来让leader更新自己
		Success bool // 如果follower包含的日志匹配参数汇总的prevLogIndex和prevLogTerm，返回true

		// OPTIMIZE: see thesis section 5.3
		ConflictTerm  int // 2C
		ConflictIndex int // 2C
	}
)

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	defer DPrintf("[AppendEntries]<Node %v｜Term %v>: role %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v> args: %v  reply: %v", rf.me, rf.currentTerm, rf.state.String(), rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	DPrintf("[AppendEntries]<Node %v| Term %v> args {%v}", rf.me, rf.currentTerm, *args)
	//如果参数term小于接收者的currentTerm，返回false,过期消息，拒绝复制
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	//消息任期大于等于，心跳生效，需要重置选举计时器，也就是刷新follower状态
	rf.becomeFollower()

	//preLogIndex比本地最早的消息还要晚，说明肯定找不到，直接拒绝并返回term=0
	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term, reply.Success = 0, false
		DPrintf("[AppendEntries] <%v|%v> receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.state, rf.me, args, args.LeaderId, args.PrevLogIndex, rf.getFirstLog().Index)
		return
	}

	//如果需要的话，算法可以通过减少被拒绝的追加条目(AppendEntries) RPC的次数来优化。
	//例如，当追加条目(AppendEntries) RPC的请求被拒绝时，跟随者可以包含冲突条目的任期号和它自己存储的那个任期的第一个索引值。
	//借助这个信息，领导者可以减少nextIndex来越过该任期内的所有冲突的日志条目；
	//这样就变为每个任期需要一条追加条目(AppendEntries) RPC而不是每个条目一条。
	//这么做之所以有效的原因在于AppendEntriesArgs的entrires携带的日志条目可以在冲突点之前，但不能在冲突点之后。也就是说，如果任期2的某个条目是冲突点，但该条目不是任期2的第一个条目，按照论文中给出的优化处理，entries中将包含从任期2的第一个条目到该冲突点之前的所有条目，而这些条目本身是和leader的log中对应位置的条目是匹配的，但是截断这些条目并替换为leader中一样的条目，仍然是正确的。

	//本地log里没有和preLogTerm以及preLogIndex相匹配的log，说明领导者那里的log除了这次发的还有没同步的，需要把之前的也同步过来
	if !rf.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		reply.Term, reply.Success = rf.currentTerm, false //拒绝接受，返回自己的term
		lastIndex := rf.getLastLog().Index
		if lastIndex < args.PrevLogIndex {
			//本地最新日志比preLog早，不能确定本地日志和领导者日志的关系，需要再次通信
			// entries before args.PrevLogIndex might be unmatch
			// return false and ask Leader to decrement PrevLogIndex
			// no conflict term
			reply.ConflictTerm, reply.ConflictIndex = -1, lastIndex+1
		} else {
			//本地最新日志比preLog晚，可以直接在跟随者这里确认冲突的任期，
			// receiver's log in certain term unmatches Leader's log
			reply.ConflictTerm = rf.getRelativeIndexLog(args.PrevLogIndex).Term

			firstLog := rf.getFirstLog()
			// 显然，因为 rf.logs[0] 确保在所有服务器之间匹配
			// ConflictIndex 必须 > 0，安全到负 1
			// 期望 Leader 检查前项
			// 所以将 ConflictIndex 设置为 ConflictTerm 中的第一个条目
			index := args.PrevLogIndex - 1
			for index >= firstLog.Index && firstLog.Term == reply.ConflictTerm {
				index--
			}

			reply.ConflictIndex = index
		}
		return
	}

	//本地log:|40-1|41-2|42-3|43-4|44-5|
	//请求log:|43-5|44-6|45-7|46-8|47-9|
	//preLogIndex=42,前面已经判断了match
	//因为绝对相信领导者，找到index=43的term不一样，发生冲突。本地log后面的都要删除，请求log 45后面的都要写入。
	firstIndex := rf.getFirstLog().Index
	for index, entry := range args.LogEntries {
		if entry.Index-firstIndex >= len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term { //args里遍历剩下的日志本地没有，直接同意更新 或者 遇到了任期不同的日志，舍弃后面的
			rf.logs = shrinkEntriesArray(append(rf.logs[:entry.Index-firstIndex], args.LogEntries[index:]...))
			break
		}
	}

	rf.advanceCommitIndexForFollower(args.LeaderCommitIndex)
	reply.Term, reply.Success = rf.currentTerm, true
}

//处理返回的结果，使用前上锁
func (rf *Raft) handleAppendEntriesResponse(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.state != StateLeader {
		return
	}
	if reply.Success {
		// successfully replicated args.LogEntries
		//已经发送的日志index向后移动
		//nextIndex为matchIndex+1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.LogEntries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// check if we need to update commitIndex
		// from the last log entry to committed one
		for i := rf.getLastLog().Index; i > rf.commitIndex; i-- {
			count := 0
			//该index已经发送给follower，则count++
			for _, matchIndex := range rf.matchIndex {
				if matchIndex >= i {
					count += 1
				}
			}
			DPrintf("[handleAppendEntriesResponse] <%v|%v> matchIndex: %v, count: %v", rf.state, rf.me, i, count)
			//超过半数
			if count > len(rf.peers)/2 {
				// most of nodes agreed on rf.logs[i]
				rf.commitIndex = i
				rf.applyCond.Signal()
				//因为index从大到小，找到个最大的满足了直接break
				DPrintf("[handleAppendEntriesResponse] <%v|%v> apply log", rf.state, rf.me)
				break
			}
		}
	} else {
		if reply.Term > rf.currentTerm {
			rf.becomeFollower()
			{
				rf.currentTerm, rf.votedFor = reply.Term, -1
				rf.persist()
			}
		} else {
			// log unmatch, update nextIndex[server] for the next trial
			rf.nextIndex[server] = reply.ConflictIndex

			// if term found, override it to
			// the first entry after entries in ConflictTerm
			if reply.ConflictTerm != -1 {
				DPrintf("%v conflict with server %d, prevLogIndex %d, log length = %d", rf, server, args.PrevLogIndex, len(rf.logs))
				for index := args.PrevLogIndex; index >= rf.getFirstLog().Index+1; index-- {
					//找到跟随者日志冲突的term的第一个index，尝试下
					//找到prelog term一样的

					if rf.getRelativeIndexLog(index-1).Term == reply.ConflictTerm {
						// in next trial, check if log entries in ConflictTerm matches
						rf.nextIndex[server] = index
						break
					}
				}
			}
			// TODO: retry now or in next RPC?
		}
	}
}

//使用前上锁
func (rf *Raft) matchLog(PrevLogTerm, PrevLogIndex int) bool {
	//本地是否有term和index一样的log
	if PrevLogIndex > rf.getLastLog().Index || PrevLogIndex < rf.getFirstLog().Index { //大于本地最大日志，小于
		return false
	}
	return rf.getRelativeIndexLog(PrevLogIndex).Term == PrevLogTerm
}

//使用前上锁
func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getFirstLog() LogEntry {
	return rf.logs[0]
}
func (rf *Raft) advanceCommitIndexForFollower(leaderCommitIndex int) {
	DPrintf("[advanceCommitIndexForFollower] <%v|%v> receives leader's leaderCommitIndex: %v,local commitIndex: %v", rf.state, rf.me, leaderCommitIndex, rf.commitIndex)
	if leaderCommitIndex > rf.commitIndex { // 领导者保证拥有所有提交的条目
		lastLogIndex := rf.getLastLog().Index
		rf.commitIndex = Min(leaderCommitIndex, lastLogIndex)
		rf.applyCond.Signal()
	}
}

//一个专门的applier goroutine，保证每条日志都会被推送到applyCh一次，确保服务的应用条目和raft的提交条目可以并行
func (rf *Raft) applier() {
	for rf.killed() == false { //循环运行
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex { //commitIndex比上一个应用到状态及的id还小，说明commitIndex还没更新
			rf.applyCond.Wait() //协程休眠
		}
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		//新的要应用到状态机的日志：[lastApplied + 1, commitIndex]
		entries := deepCopyLogEntries(rf.logs[rf.getRelativeLogIndex(lastApplied)+1 : rf.getRelativeLogIndex(commitIndex)+1])
		rf.mu.Unlock()
		//push applyCh 的过程不能够持锁
		DPrintf("[applier] <%v|%v> apply logs: %v", rf.state, rf.me, entries)
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		// 使用 commitIndex 而不是 rf.commitIndex 因为 rf.commitIndex 在 Unlock() 和 Lock() 期间可能会改变
		// 直接使用 Max(rf.lastApplied, commitIndex) 而不是 commitIndex 避免同时 InstallSnapshot rpc 导致 lastApplied 回滚
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

//本地logs属于长久的切片，在经过合并后会造成cap比len长
//切片里存放的是指针对象，那么下面删除末尾的元素后，被删除的元素依然被切片底层数组引用，从而导致不能及时被自动垃圾回收器回收
//采用复制一份大小合适的
func shrinkEntriesArray(logs []LogEntry) []LogEntry {
	shrinkLogs := make([]LogEntry, len(logs))
	copy(shrinkLogs, logs)
	logs = nil
	return shrinkLogs
}

func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogTerm:  rf.getRelativeIndexLog(prevLogIndex).Term,
		PrevLogIndex: prevLogIndex,
		//发送preLogIndex后面的日志（假设preLogIndex吻合）
		LogEntries:        deepCopyLogEntries(rf.logs[rf.getRelativeLogIndex(prevLogIndex)+1:]),
		LeaderCommitIndex: rf.commitIndex,
	}
}

func (rf *Raft) getRelativeLogIndex(index int) int {
	// index of rf.logs
	return index - rf.getFirstLog().Index
}

func (rf *Raft) getRelativeIndexLog(index int) LogEntry {
	return rf.logs[index-rf.getFirstLog().Index]
}

func deepCopyLogEntries(src []LogEntry) (dst []LogEntry) {
	n := len(src)
	dst = make([]LogEntry, n)
	copy(dst, src)
	return
}
