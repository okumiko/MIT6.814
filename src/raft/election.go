package raft

type RequestVoteArgs struct {
	Term         int //candidate的任期号
	CandidateId  int //发起投票的candidate的ID
	LastLogTerm  int
	LastLogIndex int
}

type RequestVoteReply struct {
	Term        int  //服务器的当前任期号，让candidate更新自己
	VoteGranted bool //如果是true，意味着candidate收到了选票
}

//处理请求投票handle
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() //持久化
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteArgs %v and reply requestVoteReply %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	//识别出过期的server，更新它的term
	//1.term小；2.同一个任期接到了两个投票请求，voteFor说明已经投了一个票，如果投的不是之前投的就拒绝投票(一个term只能投一次票，要不然票数都乱了)
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	//投票，变成跟随者
	if args.Term > rf.currentTerm {
		rf.ToState(StateFollower)  //当前节点改变状态
		rf.currentTerm = args.Term //更新当前节点任期
	}
	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.votedFor = args.CandidateId
	resetTimer(rf.electionTimer, RandomizedElectionTimeout()) //reset选举定时器
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//发现使用这个函数前都上锁了
func (rf *Raft) StartElection() {
	rf.currentTerm += 1 //新一轮选举将currentTerm加一
	rf.votedFor = rf.me //投给自己
	grantedVotes := 1   //投票统计,默认投给自己
	request := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, request)
	rf.persist() //持久化
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		//并行异步投票
		go func(peer int) {
			response := new(RequestVoteReply)
			if rf.sendRequestVote(peer, request, response) { //rpc调用成功返回
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, response, peer, request, rf.currentTerm)

				//用rf.currentTerm == request.Term跳过过期的请求回复
				if rf.currentTerm == request.Term && rf.state == StateCandidate {
					if response.VoteGranted { //回复同意投票
						grantedVotes += 1                   //计数加一
						if grantedVotes > len(rf.peers)/2 { //超过半数以上
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.ToState(StateLeader) //本节点变为领导者
						}
					} else if response.Term > rf.currentTerm { //回复不同意，并且收到的回复任期大于当前节点任期
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, response.Term, rf.currentTerm)
						rf.currentTerm = response.Term //更新本节点任期
						rf.ToState(StateFollower)      //本节点变为跟随者
						rf.persist()                   //持久化
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) isLogUpToDate(LastLogTerm, LastLogIndex int) bool {
	localLastLog := rf.getLastLog()
	if LastLogTerm != localLastLog.Term {
		return LastLogTerm > localLastLog.Term
	} else {
		return LastLogIndex > localLastLog.Index
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != StateLeader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index { //preLogIndex比本地firstLog还小，说明无法通过本地日志恢复，只能用快照
		// only snapshot can catch up
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.logs[0].Index,
			LastIncludedTerm:  rf.logs[0].Term,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.RUnlock()
		response := new(InstallSnapshotReply)
		//发送快照
		if rf.sendInstallSnapshot(peer, args, response) {
			rf.mu.Lock()
			rf.handleInstallSnapshotResponse(peer, args, response)
			rf.mu.Unlock()
		}
	} else { //preLogIndex可以发送
		// just entries can catch up
		request := rf.genAppendEntriesArgs(prevLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, request, response) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, response)
			rf.mu.Unlock()
		}
	}
}
