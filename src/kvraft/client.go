package kvraft

import "6.824/labrpc"

type Operation int

const (
	OpGet Operation = iota
	OpPut
	OpAppend
)

//Clerk 客户端
type Clerk struct {
	servers []*labrpc.ClientEnd //rpc客户端
	// You will have to modify this struct.
	leaderId int64 //客户端记忆leader的id

	//clientId和commandId唯一确定一个命令，多次apply相同的命令不会应用而是直接返回
	clientId  int64
	commandId int //每个命令一个顺序递增的commandId。在没有明确收到服务端写入成功或失败之前是不能改变的
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:   servers,
		leaderId:  0,
		clientId:  nrand(),
		commandId: 0,
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandRequest{Key: key, Op: OpGet})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpPut})
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpAppend})
}

//Command 统一命令执行函数
func (ck *Clerk) Command(request *CommandRequest) string {
	request.ClientId, request.CommandId = ck.clientId, ck.commandId
	for {
		//重试类型：
		//1. rpc消息到达前重新选举||初始化，未保存真正的leaderId，wrongLeader错误
		//2. 超时错误
		//3. rpc客户端没有收到服务端的回复
		//TODO: 万一网络瘫痪了，无限重试？
		var response CommandResponse
		if !ck.servers[ck.leaderId].Call("KVServer.Command", request, &response) || response.Err == ErrWrongLeader || response.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers)) //保存/尝试leaderId
			continue
		}
		ck.commandId++ //收到服务端回复了，commandId加一
		return response.Value
	}
}

type CommandRequest struct {
	Key       string
	Value     string
	ClientId  int64
	CommandId int
	Op        Operation
}
type CommandResponse struct {
	Value string
	Err   Err
	Op    Operation
}
