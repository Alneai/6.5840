# MIT 6.5840

## Lab1: MapReduce

实现思路：worker不断请求协调者，协调者中记录已完成的任务与已分配但未完成的任务分配时间。当任务完成时worker向协调者发送RPC，直至任务全部完成。

注意点：
1. 使用临时文件 `os.CreateTemp("", "tmp-")` 防止中途崩溃的worker写入文件
2. 使用 `json.NewEncoder().Encode(&kv)` 将序列化的JSON写入文件
3. 为避免频繁打开文件，键值对按照 `ReduceId` 排序，批量写入文件中

## Lab3: Raft

### leader election

注意点：
1. 对任何接收到的RPC请求或响应，若任期号 `T > currentTerm`，则令 `currentTerm = T`，并切换为跟随者状态
    - `tick()` 的计时器用来定期判断是否超过超时时间，与Raft的选举超时时间不应该一样
2. 什么时候server会投票：
    - `votedFor` 为空或者为 `candidateId`，并且候选人的日志至少和自己一样新
    - 投票后要重置自己的计时器，避免tick时间间隔问题导致的重复选举
3. 什么时候更新 `votedFor`：
    - 任期更新，变为Follower
    - 变为Candidate
    - 投票给某个Candidate
4. 发送RPC需要新开 `go routine

遇到的问题：
1. 瓜分选票，超时时间相似导致无法选出Leader，导致测试2时好时坏
2. 如果自己也为Candidate，此时收到了一个任期更新的投票请求，需要先将自己变为Follower