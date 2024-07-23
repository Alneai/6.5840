package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

/*
mapf: (filename, file) -> (key, 1)
reducef: (key, 1) -> (key, sum)
*/
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply := AssignTask()
		if reply.Done {
			log.Println("all task done!")
			break
		}

		if reply.TaskType == 1 {
			filename := reply.Filename
			if filename == "" {
				log.Fatalf("filename empty!")
			}

			// 将文件读入 content
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			log.Printf("open file %s success\n", filename)
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			// 执行mapf并写入文件
			kva := mapf(filename, string(content))
			mapId := reply.TaskId

			sort.Slice(kva, func(i, j int) bool {
				return ihash(kva[i].Key)%reply.NReduce < ihash(kva[j].Key)%reply.NReduce
			})

			// 按照ReduceId写入文件提高性能，使用json序列化
			// 使用临时文件防止worker崩溃
			var oname string
			var ofile *os.File
			var enc *json.Encoder
			lastId := -1
			for _, kv := range kva {
				nowId := ihash(kv.Key) % reply.NReduce
				if nowId != lastId {
					if ofile != nil {
						oname = fmt.Sprintf("mr-%d-%d", mapId, lastId)
						os.Rename(ofile.Name(), oname)
						log.Printf("write file %s success\n", oname)
						ofile.Close()
					}

					ofile, err = os.CreateTemp("", "tmp-")
					if err != nil {
						log.Fatalf("create %v failed", oname)
					}
					enc = json.NewEncoder(ofile)
					lastId = nowId
				}
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("json encode fail, %v", err)
				}
			}

			if ofile != nil {
				oname = fmt.Sprintf("mr-%d-%d", mapId, lastId)
				os.Rename(ofile.Name(), oname)
				log.Printf("write file %s success\n", oname)
				ofile.Close()
			}

			CompleteTask(reply.TaskType, reply.TaskId)
			log.Printf("worker on map %d done\n", mapId)
		} else {
			// 若文件存在说明此reduce已被执行过，直接返回
			reduceId := reply.TaskId
			oname := "mr-out-" + strconv.Itoa(reduceId)
			_, err := os.Stat(oname)
			if os.IsExist(err) {
				log.Printf("%v already be reduced", oname)
				continue
			}

			files, _ := getFilesBySuffix(".", "-"+strconv.Itoa(reduceId))

			kva := []KeyValue{}
			for _, filename := range files {
				file, _ := os.Open(filename)
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
			}

			sort.Slice(kva, func(i, j int) bool {
				return kva[i].Key < kva[j].Key
			})

			ofile, _ := os.CreateTemp("", "tmp-")

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			os.Rename(ofile.Name(), oname)
			ofile.Close()
			CompleteTask(reply.TaskType, reply.TaskId)
			log.Printf("worker on reduce %d done\n", reduceId)
		}

		time.Sleep(200 * time.Millisecond)
	}
}

func getFilesBySuffix(dir, suffix string) ([]string, error) {
	var fileNames []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), suffix) {
			fileNames = append(fileNames, info.Name())
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return fileNames, nil
}

// rpc发送失败则不断重试
func AssignTask() AssignTaskReply {
	args := AssignTaskArgs{}
	reply := AssignTaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	for !ok {
		log.Println("call AssignTask failed!")
		time.Sleep(2 * time.Second)
		ok = call("Coordinator.AssignTask", &args, &reply)
	}
	return reply
}

func CompleteTask(taskType, taskId int) CompleteTaskReply {
	args := CompleteTaskArgs{TaskType: taskType, TaskId: taskId}
	reply := CompleteTaskReply{}

	ok := call("Coordinator.CompleteTask", &args, &reply)
	for !ok {
		log.Println("call CompleteTask failed!")
		time.Sleep(2 * time.Second)
		ok = call("Coordinator.CompleteTask", &args, &reply)
	}
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
