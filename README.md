## Coordinator

### State Management

- 有几个Map任务，即有几个文件，记录于nMap中，并生成相应的X的中间文件
- 有几个Reduce任务，即nReduce的值是多少，生成Y，用于reduce worker决定自己处理的中间文件
- 记录每个任务的状态
    - idle
    - inprogress
    - completed

### Task Assignment

- worker发送RPC请求给coordinator
    - 先看Map：遍历所有的map任务，发现有无idle状态的task，分配给请求的worker，分配之后，将任务状态改正为inprogress，并记录处理开始的时间，以防止超时
    - 如果没有idle的map任务，需要检查是否所有的map任务都completed了
        - 如果没有都完成，告知worker需要等待才可以开始reduce
        - 如果已经都完成了，进入reduce阶段
    - 遍历所有的reduce任务，发现有无idle状态的task，分配给请求的worker并将任务状态改正为inprogress，并记录处理开始的时间，以防止超时
    - 所有的任务都完成后，告知worker可以exit
- worker完成task上报给coordinator
    - task状态如果是inporgress说明没有超时，正常更改为completed
    - task状态如果是idle说明超时了，ticker已经将其重置为idle了，则忽略此次report
    - task状态如果是completed，忽略

### Fault Tolerance

- worker可能卡住或者很慢
    - 记录每个task的开始时间
    - 后台巡逻：每隔1s检查inprogress的task，检查是否已经处理超过10s
        - 是，判定死亡，任务状态变更为idle
        - 否，继续

### Thread Safety

shared + write

- maptasklisk[]
- reducetasklisk[]
- ……

## Worker

要任务 → 干活 →交差 →循环… →结束退出

- AssignTask得到TaskType，filename，taskid，nReduce的值和nMap的值
    - Map，输入文件名、taskid和nReduce
        - 读取文件内容，解析成键值对，比如
        - call mapf() 得到kvlist
        - Bucketing，利用nReduce和hash，[][]
        - 使用json.NewEncoder写入intermediate files，以便在 reduce 任务期间能够正确读取。为了确保在程序崩溃时不会有人看到部分写入的文件，MapReduce 论文提到了一种技巧：使用临时文件，并在文件完全写入后原子地重命名它。您可以使用`ioutil.TempFile`创建一个临时文件，并使用`os.Rename`原子地重命名它。
    - Reduce，输入taskid和nMap
        - 读取所有以某Y结尾的文件，打开文件，json.NewDecoder
        - sort.Sort
        - 双指针计数一下
        - call reducef()
        - 写入mr-out-Y的计数结果，记得用temporary file
- TaskDone报告完成

## RPC

定义通信结构体

### Worker → Coordinator

- ask
- report
    - 完成的是哪个任务？taskid
    - 完成的是哪个任务的类型？tasktype

### Coordinator → Worker

- 做哪个类型的任务？ tasktype
- 做的是具体的哪个任务？taskid
- 如果是map，还需要告诉filename
- 如果是map，还需要告知nreduce用于hash取模
- 如果是reduce，需要告诉nmap用于遍历处理相同reduce taskid的中间件

## Example Function

- RPC
    - args struct    //input
    - reply struct    //output
- worker
    - Call
        - 建造args结构体，放入需要的数据
        - 建造reply结构体
        - 阻塞等待，ok := call("Coordinator.Example", &args, &reply)
        - if ok，则wake up
- coordinator
    - 当worker阻塞时，coordinator收到数据
    - 放入reply的数据，worker收到

## Advanced

### RPC

- 废弃 Unix Domain Socket（/var/tmp/xxx），改用 TCP 协议
- Coordinator：监听固定端口（1234），等待 Worker 连接
- Worker：启动时通过命令行参数接收 Coordinator 的 IP:Port，通过 TCP 发起 RPC 调用
- Worker 启动时，动态获取本机对外 IP 地址
- Worker 申请一个随机的空闲端口（:0），用于后续的数据传输服务。

### Map

- 每个 Worker 启动一个 HTTP File Server。
- 该 Server 负责响应外部请求，读取本地磁盘上的中间文件并返回文件内容
- Map Worker 向 Coordinator 汇报 TaskDone 时，还要汇报自己的地址

### Reduce

- Reduce Worker 不再使用 os.Open 读取文件。
- 它根据 Coordinator 提供的地址列表，遍历所有 Map Worker。
- 对每个 Map Worker 发起 HTTP GET 请求，下载属于当前 Reduce 任务编号的 JSON 数据（http://<WorkerIP>/mr-0-1 ）

### Coordinator

- Coordinator 需要维护一个list，记录每个已完成的 Map 任务对应的 Worker 地址
- 当分配 Reduce 任务时，Coordinator 必须将这份“地址簿”（所有 Map 任务的下载位置列表）打包在 RPC 回复中发送给 Reduce Worker

### Fault Tolerance

- Worker A 完成了 Map 任务并保存了数据，然后 Worker A 宕机（被 Kill）
- Worker B 捕获下载错误，向 Coordinator 发送一个特殊的 RPC
- Coordinator 收到报告后，将 Map 任务 X 的状态从 Completed 强制重置为 Idle
- Coordinator 将该 Map 任务重新分配给其他存活的 Worker

### 本地测试
`cd src/main`
`go build -buildmode=plugin ../mrapps/wc.go`
`rm mr-out*`

启动coordinator：
`go run mrcoordinator.go pg-*.txt`

一个命令窗口启动worker1：
`go run mrworker.go 127.0.0.1:1234 wc.so`
跑完几个 Map 任务（屏幕显示 Map Task X completed）但还没进入 Reduce 阶段时，在终端 2 按 Ctrl+C kill

一个命令窗口启动worker 2：
`go run mrworker.go 127.0.0.1:1234 wc.so`
Worker 2 会重新执行那些丢失的 Map 任务

`cat mr-out-* | sort | more`

### Cloud Test
`ssh -i ~/DS_Lab2.pem ubuntu@xx.xx.xx.xx`
`./mrcoordinator pg-*.txt`
`./mrworker coordinator_ip:1234 wc.so`
