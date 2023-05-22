package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// 以下是对RPC过程中用到的一些数据结构
// https://www.cnblogs.com/amuge/articles/16256217.html
// rpc的server的接收函数，其函数名称和所有形参的第一个字母必须大写，
// 形参用到的结构体的参数，第一个字母也要大写
type GetTaskReply struct {
	Task     Task
	WorkerId int
}

type RegWorkerReply struct {
	WorkerId int
	NMap     int
	NReduce  int
}

type ReportWorkerReply struct {
}

type GetTaskArgs struct {
	WorkerId int
}

type RegWorkerArgs struct {
	A int
}

type ReportWorkerArgs struct {
	Done   bool
	TaskId int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
