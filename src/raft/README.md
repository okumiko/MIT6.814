# Raft

## 持久化

```go
	currentTerm int        //服务器知道的最近任期，当服务器启动时初始化为0
	votedFor    int        //当前任期中，该服务器给投过票的candidateId，如果没有则为null
	logs        []LogEntry //日志条目；每一条包含了状态机指令以及该条目被leader收到时的任期号
	// 第一个条目是一个虚拟条目，其中包含 LastSnapshotTerm、LastSnapshotIndex 和 nil 命令
