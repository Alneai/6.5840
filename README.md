# MIT 6.5840

## Lab1: MapReduce

实现思路：worker不断请求协调者，协调者中记录已完成的任务与已分配但未完成的任务分配时间。当任务完成时worker向协调者发送rpc，直至任务全部完成。

注意点：
1. 使用临时文件 `os.CreateTemp("", "tmp-")` 防止中途崩溃的worker写入文件
2. 使用 `json.NewEncoder().Encode(&kv)` 将序列化的json写入文件
3. 为避免频繁打开文件，键值对按照 `ReduceId` 排序，批量写入文件中