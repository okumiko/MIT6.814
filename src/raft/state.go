package raft

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

// StateType represents the role of a node in a cluster.
type StateType int

var stmap = [...]string{
	"FOLLOWER",
	"CANDIDATE",
	"LEADER",
}

func (st StateType) String() string {
	return stmap[int(st)]
}

//状态转换函数，用之前必须上锁
//完成过渡，返回时已经成为了崭新的确定状态
func (rf *Raft) ToState(state StateType) {
	if state == rf.state {
		rf.holdState()
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

func (rf *Raft) holdState() {
	switch rf.state {
	case StateLeader:
	case StateCandidate:
		rf.StartElection()                                        //重新竞举
		resetTimer(rf.electionTimer, RandomizedElectionTimeout()) //重置选举定时器计时（随机时间）
	case StateFollower: //维持跟随者状态需要重置选举定时器
		rf.votedFor = -1
		resetTimer(rf.electionTimer, RandomizedElectionTimeout()) //重置选举定时器计时（随机时间）
	}
}
