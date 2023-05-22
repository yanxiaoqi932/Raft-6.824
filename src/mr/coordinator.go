package mr

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type CoordinatorPhase int
type TaskStatus int

const ( // 定义系统阶段
	CoordinatorPhase_Map    = 0
	CoordinatorPhase_Reduce = 1
)

const ( // 定义任务阶段
	TaskPhase_Map    = 0
	TaskPhase_Reduce = 1
)

const ( // 定义任务状态
	TaskStatus_New     = 0 // 理论上存在，但还没有创建
	TaskStatus_Ready   = 1 // 刚刚创建好
	TaskStatus_Running = 2
	TaskStatus_End     = 3
	TaskStatus_Error   = 4
)

const ( // 定义扫描间隔时间和任务最大执行时间
	ScanInterval       = time.Microsecond * 500
	MaxTaskRunningTime = time.Second * 5
)

type Task struct {
	Filename  string
	Status    TaskStatus
	Alive     bool
	WorkerId  int
	TaskId    int
	TaskPhase int
	StartTime time.Time // 任务开始running的时间
}

type Coordinator struct { // 定义调度中心
	// Your definitions here.
	phase     int
	nMap      int
	nReduce   int
	fileQueue []string
	taskQueue []Task
	taskChan  chan Task // 将ready后的任务发给worker
	workerId  int       // 每注册创建一个worker，workerQueue就加1作为该worker的id
	done      bool
	muLock    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		DPrintf("listen error:", e)
	}
	go http.Serve(l, nil)

	// if err != nil {
	// 	fmt.Println(err.Error())
	// }
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// 这个可以直接调用c.done，为什么要包装成函数
func (c *Coordinator) Done() bool {
	c.muLock.Lock()
	defer c.muLock.Unlock()
	ret := false
	// Your code here.
	ret = c.done
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.phase = CoordinatorPhase_Map
	c.nMap = len(files)
	c.nReduce = nReduce
	c.fileQueue = files
	c.taskQueue = make([]Task, len(files))
	c.done = false
	c.workerId = 0
	if len(files) > nReduce { // 通道槽数选最大的，避免阻塞
		c.taskChan = make(chan Task, len(files))
	} else {
		c.taskChan = make(chan Task, nReduce)
	}

	go c.scan()
	c.server()
	return &c
}

func (c *Coordinator) scan() {
	for !c.Done() {
		c.SpecificScan()
		// DPrintf("coordinator starts scan...")
		time.Sleep(ScanInterval)
	}
}

func (c *Coordinator) SpecificScan() {
	c.muLock.Lock()
	defer c.muLock.Unlock()

	// 开始扫描
	allDone := true
	for k, task := range c.taskQueue {
		switch task.Status {
		case TaskStatus_New:
			allDone = false
			c.taskChan <- c.NewOneTask(k)
			c.taskQueue[k].Status = TaskStatus_Ready
		case TaskStatus_Ready:
			allDone = false
		case TaskStatus_Running:
			allDone = false
			// 如果超时，就重置该任务为ready
			if time.Since(task.StartTime) > MaxTaskRunningTime {
				DPrintf("task %d timeout", task.TaskId)
				c.taskQueue[k].Status = TaskStatus_Ready
				c.taskChan <- c.NewOneTask(k)
			}
		case TaskStatus_End:
		case TaskStatus_Error:
			allDone = false
			// 重置该任务状态，并且重新传送一个task过去
			DPrintf("task %d error", task.WorkerId)
			c.taskQueue[k].Status = TaskStatus_Ready
			c.taskChan <- c.NewOneTask(k)
		default:
			allDone = false
			DPrintf("没有扫描到任务状态！")
		}
	}
	if allDone {
		if c.phase == CoordinatorPhase_Map {
			c.phase = CoordinatorPhase_Reduce
			c.taskQueue = make([]Task, c.nReduce)
			DPrintf("进入reduce阶段...")
		} else {
			c.done = true
			DPrintf("成功完成任务！")
		}
	}
}

func (c *Coordinator) NewOneTask(taskId int) Task {
	task := Task{
		Filename:  "",
		Status:    TaskStatus_Ready,
		Alive:     true,
		TaskId:    taskId,
		TaskPhase: c.phase,
	}
	if task.TaskPhase == TaskPhase_Map {
		task.Filename = c.fileQueue[taskId]
	}
	DPrintf("new one task...")
	return task
}

// 处理RPC请求：
// worker请求task时，将task创建时未补齐的信息补齐，
// 然后用reply引用传递给worker
func (c *Coordinator) SendOneTask(args *GetTaskArgs, reply *GetTaskReply) error {
	DPrintf("send a task...")
	task := <-c.taskChan
	reply.Task = task
	if task.Alive {
		c.muLock.Lock()
		if task.TaskPhase != c.phase {
			DPrintf("task %d phase error!", task.TaskId)
			return errors.New("error task phase")
		}
		reply.Task.Status = TaskStatus_Running
		reply.Task.WorkerId = args.WorkerId
		reply.Task.StartTime = time.Now()
		c.taskQueue[task.TaskId].StartTime = time.Now()
		c.taskQueue[task.TaskId].WorkerId = args.WorkerId
		c.taskQueue[task.TaskId].Status = TaskStatus_Running
		c.muLock.Unlock()
	}
	return nil
}

// 处理RPC请求：
// worker创建时需要调用注册函数来获取自身的workerid
func (c *Coordinator) RegOneWorker(args *RegWorkerArgs, reply *RegWorkerReply) error {
	c.muLock.Lock()
	defer c.muLock.Unlock()
	DPrintf("register a worker...")
	println("c.nmap:%d, c.nreduce:%d", c.nMap, c.nReduce)
	reply.WorkerId = c.workerId
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	c.workerId++
	return nil
}

// 处理RPC请求
// coodinator收到worker的report，更新task的status
func (c *Coordinator) ReportWorker(args *ReportWorkerArgs, reply *ReportWorkerReply) error {
	c.muLock.Lock()
	DPrintf("get report from worker...")
	if args.Done {
		c.taskQueue[args.TaskId].Status = TaskStatus_End
	} else {
		DPrintf("%dth task error!", args.TaskId)
		c.taskQueue[args.TaskId].Status = TaskStatus_Error
	}
	c.muLock.Unlock()
	return nil
}

var file *os.File

// const putInLog = false

func InitLog() {
	f, err := os.Create("log-" + time.Now().Format("01-02-03-04") + ".txt")
	if err != nil {
		DPrintf("write log error!")
	}
	file = f
}

func DPrintf(s string, value ...interface{}) {
	now := time.Now()
	info := fmt.Sprintf("%v-%v %v:%v:%v:  ", int(now.Month()), now.Day(), now.Hour(), now.Minute(), now.Second()) + fmt.Sprintf(s+"\n", value...)

	file.WriteString(info)
	fmt.Printf(info)

}
