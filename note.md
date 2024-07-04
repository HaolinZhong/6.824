# Background
- sequential mapreduce implementation: `src/main/mrsequential.go`
- MapReduce applications: `mrapps/wc.go`, `mrapps/indexer.go`
  - run word count:
```
  cd src/main
  go build -race -buildmode=plugin ../mrapps/wc.go
  rm mr-out*
  go run -race mrsequential.go wc.so pg*.txt
  more mr-out-0
```

- test: `main/test-mr.sh`

- change `ret := false` to true in the `Done` function in `mr/coordinator.go` so that the coordinator exits immediately.
- The test script expects to see output in files named `mr-out-X`, one for each reduce task. The empty implementations of `mr/coordinator.go` and `mr/worker.go` don't produce those files (or do much of anything else), so the test fails.


# Rules

- map phase中, 应将intermediate key 分到不同bucket中, 这是为了`nReduce`个reduce task而做的.
  - `nReduce` 是传给`mrcoordinator.go`的命令行参数
- worker要将第X个reduce task的输出放到`mr-out-X`文件中
- `mr-out-X`文件
  - 对每个reduce func的输出, 都有对应的一行. 行内用`"%v %v"`格式来format key和value
- 实际工作的文件
  - `mr/worker.go`
  - `mr/coordinator.go`
  - `mr/rpc.go`
- worker要把中间任务的map输出到文件, 以供后续worker再读文件进行reduce
- `mrcoordinator.go`需要`coordingator.go`来实现`Done()`方法. 这个方法应在MapReduce job完全完成时return true. 然后`mrcoordinator.go`就会退出
- 当任务完全完成时, worker进程应当退出.
  - 简单的实现方式是, 使用`call()`的返回值. 如果worker没能联系上coordinator, 那么他就可以假定coordinator因为任务完成已退出.
  - 基于个人的设计, 也可以设计一个pseudo task传达exit信息来让worker退出


# Hints

- 修改`mr/worker.go`的`Worker()`来通过rpc向coordinator要task.
  - 然后coordinator回复一个文件名, 作为一个还未初始化的map任务
  - 然后modify worker, 让worker来读取文件, 加载mr app提供的map func并返回结果 (这就需要每次重新编译)
- 这个lab实际上是依赖于文件处于同一文件系统下的. 而google当初实现时则有GFS这样的分布式文件系统
- 一个合理的intermediate文件的命名规则是, `mr-X-Y`, 其中X是map task的number, Y是reduce task的number
- 可以以json格式将worker产生的intermediate的map执行结果放在文件里
- worker的map部分可以使用`ihash(key)`函数来拿到对应一个given key的reduce task
- 可以用`mrsequential.go`里的代码来读map input文件, 在map和reduce的间隙sort intermediate K/V, 存储reduce结果到文件
- coordinator是并发的, 所以上锁保护
- 使用-race来启用go的race detector来检测死锁
- worker有时需要等待. reduce只能在最后一个map任务完成后开始.可以sleep, 或者worker调coordinator rpc时, coordinator让当前协程阻塞.
- coordinator是无法知道worker的真实情况的. 只能让coordinator等待一段时间, 若某worker则假定其fail, 将任务给另一个worker.
  - 这个lab中, 只要coordinator down了10s, 就应该假设worker die了
- 为了测试crash recovery, 用 `mrapps/crash.go` 作为mr app.
- worker在执行写入文件时, 文件应当是tempFile (`ioutil.TempFile`), 在完成写入后应该再重命名. 这样, 如果写入过程中出错, 写了一半的东西不会被当成正式结果.



# 运行

```bash
$ go build -race -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
# 启动coordinator 主程序
$ go run -race mrcoordinator.go pg-*.txt
 
# 在其他窗口启动worker
$ go run -race mrworker.go wc.so

# 运行结束后
$ cat mr-out-* | sort | more
```



# 测试

```bash
$ cd ~/6.824/src/main
$ bash test-mr.sh
*** Starting wc test.
```



# 设计

- map: 
  - map phase: 由worker来执行plugin提供的map函数来对分配的文件计算结果
  - map函数: 接收任意输入, 产生多个k, v pair (也可以理解为产生一个map), 这个map放在intermediate file中
  - 一个key对应的真实结果可能"分散"到了多个worker计算得到的多个map函数的结果中
  - map结束后需要告知中间结果的储存位置
- reduce:
  - 所有map执行完毕后才执行reduce
  - coordinator给worker发送reduce任务. 每个任务包括intermediate file存放的位置
  - 进行多次reduce. 每轮reduce拿到对同一个key的所有的结果(这些结果本来分散在多个worker中), 根据reduce函数整合多个结果.
    - worker拿到reduce任务后, 从其他worker拿到intermediate file. 对多个file做整合, 按key键sort 拍戏, 得到key - valueList. 然后对valueList reduce.
  - reduce拿到结果数据后, 再返回, 整合.

# 流程
- 启动coordinator. coordinator拿到文件列表.

- 启动worker. worker找coordinator要任务. 

  - 任务对象
    - map任务Id
    - nReduce
    - 文件名
    - 状态 (int)
      - -1: 未分配
      - 0: 被分配
      - 1: 已完成
    - 被分配时间

- coordinator收到文件请求后, 就回复任务对象. 若所有任务已完成分配, 就返回错误.

  - 每次发送一个任务对象时, coordinator都需要将其加入到pending区, 并且对pending区的任务对象进行检查, 追踪完成状态. 

  - 如果一个任务长期未完成, 则需要将其放回到 未执行区, 等待下一次worker来取任务

    - 这个检查任务专门用另一个线程来执行. (或者也可以在每次分配一个任务时, 就用定时器执行一个检查函数, 10s后生效)

  - 当worker完成一个任务后, 需要执行rpc来通知coordinator任务完成. coordinator收到消息后, 将任务放入完成区.

    

- 一个worker会执行多次map任务. 每次map任务的执行结果都会是一个kv pair的list. 

  - 对这个list中的每个k, v pair, 按照题目的要求, 都需要通过对k进行hash % nReduce来得知, 这个kv pair所属的reduce bucket number.
  - 每次执行完一个map, 就创建nReduce个文件将结果写入. 文件名为`mr-X-Y`, X是任务id, Y是每个k v pair通过上一条得知的bucket number.







- reduce phase
  - reduce phase任务的数量实际是由外部输入的nReduce来提供的
  - coordinator使用数据结构来管理reduce task任务状态. 因为要fault tolerance.重试机制.
    - 类似于map任务, 每个reduce任务都有3个状态
      - -1: 未分配
      - 0: 被分配
      - 1: 已完成
  - 一个worker拿到reduce任务后, 根据任务编号, 基于我们的文件格式, 可以找到所有的中间文件.
    - 从所有文件中拿到多个kv pair. 
    - 因为pair是从多个文件中拿到的, 所以要做排序.
    - 然后对kv pair应用reduce函数. 拿到这个reduce task的结果.
  - 存疑: nReduce个结果最后还要经过最后一次reduce. 这个final reduce也要由一个worker完成.
