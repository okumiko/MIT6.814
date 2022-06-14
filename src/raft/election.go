package raft

import "time"

type RequestVoteArgs struct {
	Term        uint32 //candidate的任期号
	CandidateId int    //发起投票的candidate的ID
}

type RequestVoteReply struct {
	Term        uint32 //服务器的当前任期号，让candidate更新自己
	VoteGranted bool   //如果是true，意味着candidate收到了选票
}

//处理请求投票handle
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() //持久化
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing requestVoteArgs %v and reply requestVoteReply %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	//识别出过期的server，更新它的term
	//1.term小；2.同一个任期接到了两个投票请求，voteFor说明已经投了一个票，如果投的不是之前投的就拒绝投票
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	//投票，变成跟随者
	if args.Term > rf.currentTerm {
		rf.ChangeState(StateFollower) //当前节点改变状态
		rf.currentTerm = args.Term    //更新当前节点任期
	}
	//if !rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
	//	response.Term, response.VoteGranted = rf.currentTerm, false
	//	return
	//}
	rf.votedFor = args.CandidateId
	resetTimer(rf.electionTimer, RandomizedElectionTimeout()) //reset选举定时器
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) StartElection() {
	request := rf.genRequestVoteArgs()
	DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, request)

	rf.currentTerm += 1 //新一轮选举将currentTerm加一
	rf.votedFor = rf.me //投给自己
	grantedVotes := 1   //投票统计,默认投给自己

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
							rf.ChangeState(StateLeader) //本节点变为领导者
						}
					} else if response.Term > rf.currentTerm { //回复不同意，并且收到的回复任期大于当前节点任期
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, response.Term, rf.currentTerm)
						rf.currentTerm = response.Term //更新本节点任期
						rf.ChangeState(StateFollower)  //本节点变为跟随者
						rf.persist()                   //持久化
					}
				}
			}
		}(peer)
	}
}

//状态转换函数，用之前必须上锁
func (rf *Raft) ChangeState(state StateType) {
	if state == rf.state {
		return
	}
	DPrintf("Term %d: server %d change state from %v to %v\n", rf.currentTerm, rf.me, rf.state, state)
	rf.state = state

	switch state { //转换状态后执行的操作
	case StateFollower: //转换为跟随者
		rf.heartbeatTimer.Stop()                                  //停止发送心跳
		rf.votedFor = -1                                          //初始化voteFor
		resetTimer(rf.electionTimer, RandomizedElectionTimeout()) //重置选举定时器计时（随机时间）
	case StateCandidate:
		//转为candidate之后，开始选举：
		//增加currentTerm
		//为自己投票
		//重设选举定时器
		//给所有的服务器发送RequestVote RPC
		rf.StartElection() //开始竞举
		resetTimer(rf.electionTimer, RandomizedElectionTimeout())
	case StateLeader: //转换为领导者
		rf.electionTimer.Stop()                                 //停止竞举定时器
		rf.BroadcastHeartbeat(true)                             //广播心跳
		resetTimer(rf.heartbeatTimer, StableHeartbeatTimeout()) //重置心跳超时时间
	}
}

func RandomizedElectionTimeout() time.Duration {

}

func StableHeartbeatTimeout() time.Duration {

}

//利用一个select来包裹channel drain，这样无论channel中是否有数据，drain都不会阻塞住
func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C: //try to drain from the channel
		default:
		}
	}
	timer.Reset(d)
}
