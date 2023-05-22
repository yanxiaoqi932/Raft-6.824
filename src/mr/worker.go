package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	task     Task
	workerId int
	nMap     int
	nReduce  int
	mapF     func(string, string) []KeyValue
	reduceF  func(string, []string) string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := worker{
		mapF:    mapf,
		reduceF: reducef,
	}
	worker.Register()
	worker.run()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func (w *worker) run() {
	for {
		err := w.GetTask()
		if err != nil {
			DPrintf("worker: get task error!")
			continue
		}
		if !w.task.Alive {
			DPrintf("task ended")
			return
		}
		switch w.task.TaskPhase {
		case TaskPhase_Map:
			w.doMapTask()
		case TaskPhase_Reduce:
			w.doReduceTask()
		default:
			panic(fmt.Sprintf("task phase err: %v", w.task.TaskPhase))
		}
	}
}

func (w *worker) getMapOutputFileName(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-kv-%d-%d", mapId, reduceId)
}

func (w *worker) getReduceInputFileName(nMap int, reduceTaskId int) []string {
	fileName := []string{}
	for i := 0; i < nMap; i++ {
		fileName = append(fileName, fmt.Sprintf("mr-kv-%d-%d", i, reduceTaskId))
	}
	return fileName
}

func (w *worker) getReduceOutputFileName(reduceTaskId int) string {
	return fmt.Sprintf("mr-out-%d", reduceTaskId)
}

func (w *worker) doMapTask() {
	DPrintf("%dth worker doing map task", w.workerId)
	content, err := ioutil.ReadFile(w.task.Filename)
	if err != nil {
		DPrintf("read task file %s error", w.task.Filename)
		w.Report(false)
	}

	kvs := w.mapF(w.task.Filename, string(content)) // 获取kvs这一KeyValue结构体数组
	partionKV := make([][]KeyValue, w.nReduce)      // 二维KeyValue数组，每一行一个分区，一个区对应一个reduce任务
	for _, v := range kvs {                         // 将所有key哈希后，分成nReduce个分区存在partion中
		partionId := ihash(v.Key) % w.nReduce
		partionKV[partionId] = append(partionKV[partionId], v)
	}
	// 将分区数据存到reduce输入文件里面
	for reduceId, reduceFileContent := range partionKV {
		fileName := w.getMapOutputFileName(w.task.TaskId, reduceId)
		// 创建文件
		file, err := os.Create(fileName)
		if err != nil {
			DPrintf("%dth task create file error", w.task.TaskId)
			w.Report(false)
			return
		}
		// 将该分区的信息存入文件中
		for _, kv := range reduceFileContent {
			encoder := json.NewEncoder(file)
			if err := encoder.Encode(&kv); err != nil { // 将结构体信息编码存入json文件中
				DPrintf("%dth task output file error", w.task.TaskId)
				w.Report(false)
				return
			}
		}
		// 关闭分区文件
		if err := file.Close(); err != nil {
			DPrintf("%dth task close file error", w.task.TaskId)
			w.Report(false)
			return
		}
	}
	w.Report(true)
}

func (w *worker) doReduceTask() {
	DPrintf("%dth worker doing reduce task", w.workerId)
	// key是string，即keyvalue中的key，value是string数组，即keyvalue中的value数组
	maps := make(map[string][]string)
	// reduce worker读取map输出的对应分区文件
	for _, fileName := range w.getReduceInputFileName(w.nMap, w.task.TaskId) {
		file, err := os.Open(fileName)
		if err != nil {
			DPrintf("%dth task open error")
			w.Report(false)
			return
		}
		// 开始读取该分区文件的数据
		decoder := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			if maps[kv.Key] == nil {
				maps[kv.Key] = []string{} // 如果key对应的value数组还没建立就建立一个
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	// 对每个key下的string数组执行reduce操作
	res := []string{}
	for k, v := range maps {
		len := w.reduceF(k, v)
		res = append(res, fmt.Sprintf("%v %v\n", k, len))
	}
	// 写入并输出文件
	fileName := w.getReduceOutputFileName(w.task.TaskId)
	if err := ioutil.WriteFile(fileName, []byte(strings.Join(res, "")), 0600); err != nil { // 0600代表权限，root可读写不可执行
		DPrintf("%dth task output error", w.task.TaskId)
		w.Report(false)
		return
	}
	w.Report(true)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	conn, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer conn.Close()

	err = conn.Call(rpcname, args, reply) //rpcname = 结构体名.方法名
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false

}

// 发送RPC请求：
// 注册一个worker
func (w *worker) Register() error {
	DPrintf("worker: register a worker...")
	var args RegWorkerArgs
	var regWorkerReply RegWorkerReply
	if ok := call("Coordinator.RegOneWorker", &args, &regWorkerReply); !ok {
		DPrintf("worker: get task error!")
	}
	w.workerId = regWorkerReply.WorkerId
	w.nMap = regWorkerReply.NMap
	w.nReduce = regWorkerReply.NReduce
	// client.Close()
	DPrintf("w.nmap:%d, w.nreduce:%d", regWorkerReply.NMap, regWorkerReply.NReduce)

	return nil
}

// 发送RPC请求：
// 请求一个task
func (w *worker) GetTask() error {
	DPrintf("worker: want to get task...")
	args := GetTaskArgs{
		WorkerId: w.workerId,
	}
	getTaskReply := GetTaskReply{}
	if ok := call("Coordinator.SendOneTask", &args, &getTaskReply); !ok {
		DPrintf("worker: get nil task...")
	}
	w.task = getTaskReply.Task
	return nil
}

// 发送RPC请求：
// 执行任务后发送report
func (w *worker) Report(done bool) {
	DPrintf("worker: report task...")
	args := ReportWorkerArgs{
		Done:   done,
		TaskId: w.task.TaskId,
	}
	reportReply := ReportWorkerReply{}
	if ok := call("Coordinator.ReportWorker", &args, &reportReply); !ok {
		DPrintf("worker: report error!")
	}
}
