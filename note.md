# MapReduce



## Background

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



## Rules

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



## Hints

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



## 运行

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



## 测试

```bash
$ cd ~/6.824/src/main
$ bash test-mr.sh
*** Starting wc test.
```



## 设计

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



## 流程

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





# Raft A



## Introduction

- Raft, a replicated state machine protocol
  - Raft organizes client requests into a sequence, called the log, and ensures that all the replica servers see the same log.
  - Each replica executes client requests in log order, applying them to its local copy of the service's state.
  - Since all the live replicas see the same log contents, they all execute the same requests in the same order, and thus continue to have identical service state.
  - If a server fails but later recovers, Raft takes care of bringing its log up to date. Raft will continue to operate as long as at least a majority of the servers are alive and can talk to each other.
  - If there is no such majority, Raft will make no progress, but will pick up where it left off as soon as a majority can communicate again.



- implement Raft as a Go object type with associated methods
  - A set of Raft instances talk to each other with RPC to maintain replicated logs.
  - Your Raft interface will support an indefinite sequence of numbered commands, also called log entries.
    - The entries are numbered with `*index numbers*`. The log entry with a given index will eventually be committed.
    - At that point, your Raft should send the log entry to the larger service for it to execute.
  - follow the design in the [extended Raft paper](http://nil.csail.mit.edu/6.824/2021/papers/raft-extended.pdf), with particular attention to Figure 2. You'll implement most of what's in the paper, including saving persistent state and reading it after a node fails and then restarts.
    - You will not implement cluster membership changes (Section 6).



- You may find this [guide](https://thesquareplanet.com/blog/students-guide-to-raft/) useful, as well as this advice about [locking](http://nil.csail.mit.edu/6.824/2021/labs/raft-locking.txt) and [structure](http://nil.csail.mit.edu/6.824/2021/labs/raft-structure.txt) for concurrency. For a wider perspective, have a look at Paxos, Chubby, Paxos Made Live, Spanner, Zookeeper, Harp, Viewstamped Replication, and [Bolosky et al.](http://static.usenix.org/event/nsdi11/tech/full_papers/Bolosky.pdf) (Note: the student's guide was written several years ago, and part 2D in particular has since changed. Make sure you understand why a particular implementation strategy makes sense before blindly following it!)



- We also provide a [diagram of Raft interactions](http://nil.csail.mit.edu/6.824/2021/notes/raft_diagram.pdf) that can help clarify how your Raft code interacts with the layers on top of it.

![image-20240704160427183](note.assets/image-20240704160427183.png)





## Get Started

- skeleton code `src/raft/raft.go`

- The tests are in `src/raft/test_test.go`.

- run test

  ```shell
  $ cd ~/6.824
  $ git pull
  ...
  $ cd src/raft
  $ go test -race
  Test (2A): initial election ...
  --- FAIL: TestInitialElection2A (5.04s)
          config.go:326: expected one leader, got none
  Test (2A): election after network failure ...
  --- FAIL: TestReElection2A (5.03s)
          config.go:326: expected one leader, got none
  ...
  $
  ```



## code

- implementation must support the following interface

  ```go
  // create a new Raft server instance:
  rf := Make(peers, me, persister, applyCh)
  
  // start agreement on a new log entry:
  rf.Start(command interface{}) (index, term, isleader)
  
  // ask a Raft for its current term, and whether it thinks it is leader
  rf.GetState() (term, isLeader)
  
  // each time a new entry is committed to the log, each Raft peer
  // should send an ApplyMsg to the service (or tester).
  type ApplyMsg
  ```

  - A service calls `Make(peers,me,…)` to create a Raft peer. The peers argument is an array of network identifiers of the Raft peers (including this one), for use with RPC. The `me` argument is the index of this peer in the peers array.
  - `Start(command)` asks Raft to start the processing to append the command to the replicated log. `Start()` should return immediately, without waiting for the log appends to complete. 
    - The service expects your implementation to send an `ApplyMsg` for each newly committed log entry to the `applyCh` channel argument to `Make()`.

  

- RPC example

  - `raft.go` contains example code that sends an RPC (`sendRequestVote()`) and that handles an incoming RPC (`RequestVote()`).
  - Your Raft peers should exchange RPCs using the labrpc Go package (source in `src/labrpc`).
  - make sure your Raft works with the original `labrpc`



- Your Raft instances must interact only with RPC; for example, they are not allowed to communicate using shared Go variables or files.



## Task

- Implement Raft leader election and heartbeats (`AppendEntries` RPCs with no log entries). 
- The goal for Part 2A is for a single leader to be elected, for the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost.



## Test

Run `go test -run 2A -race` to test your 2A code.



## Hint

- 不能直接run raft的实现, 只能借助test来run来检验.
- 查看paper的figure 2. 这个lab需要关心RequestVote的call, 选举相关的Rules 和 state.
- 将figure2中的state加到Raft struct中. 
- 需要定义struct来记录每个log entry的信息.
- 在`RequestVoteArgs`和`RequestVoteReply`中定义好所需的信息.
- 修改make方法, 来创建一个background的goroutine来检查peer 心跳.
  - 一旦超过一定时间没有收到心跳后, 就要触发选举, 调用`RequestVote`
  - 如果此时已经有leader, peer就应该知道谁是leader. 否则, 发出请求的自己就要成为leader(?).
- 心跳
  - To implement heartbeats, define an `AppendEntries` RPC struct (though you may not need all the arguments yet), and have the leader send them out periodically. 
  - Write an `AppendEntries` RPC handler method that **resets** the election timeout so that other servers don't step forward as leaders when one has already been elected.
  - Make sure the election timeouts in different peers don't always fire at the same time, or else all peers will vote only for themselves and no one will become the leader.
  - The tester requires that the leader **send heartbeat RPCs no more than ten times per second.**



- 5s时间限制
  - The tester requires your Raft to elect a new leader within five seconds of the failure of the old leader (if a majority of peers can still communicate). 
  - leader election may require multiple rounds in case of a split vote (which can happen if packets are lost or if candidates unluckily choose the same random backoff times).
  - You must pick election timeouts (and thus heartbeat intervals) that are short enough that it's very likely that an election will complete in less than five seconds even if it requires multiple rounds.



- 合适的election timeouts
  - The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds. Such a range only makes sense if the leader sends heartbeats considerably more often than once per 150 milliseconds. Because the tester limits you to 10 heartbeats per second, you will have to use an election timeout larger than the paper's 150 to 300 milliseconds, but not too large, because then you may fail to elect a leader within five seconds.



- 定时任务的最佳实现方式
  - You'll need to write code that takes actions periodically or after delays in time. The easiest way to do this is to create a goroutine with a loop that calls [time.Sleep()](https://golang.org/pkg/time/#Sleep); (see the `ticker()` goroutine that `Make()` creates for this purpose). Don't use Go's `time.Timer` or `time.Ticker`, which are difficult to use correctly.



- Don't forget to implement `GetState()`.



- kill
  - The tester calls your Raft's `rf.Kill()` when it is permanently shutting down an instance. 
  - You can check whether `Kill()` has been called using `rf.killed()`. 
  - You may want to do this in all loops, to avoid having dead Raft instances print confusing messages.



## Paper



### State





## 思想

- 通过log的一致保证多台节点的状态一致
- log强制ack - 在所有节点都确认拿到log前, 不返回操作结果
  - VMware FT中的 **output rule**: 
    - 
      客户端输入到达Primary。
    - Primary的VMM将输入的拷贝发送给Backup虚机的VMM。所以有关输入的Log条目在Primary虚机生成输出之前，就发往了Backup。之后，这条Log条目通过网络发往Backup，但是过程中有可能丢失。
    - Primary的VMM将输入发送给Primary虚机，Primary虚机生成了输出。现在Primary虚机的里的数据已经变成了11，生成的输出也包含了11。但是VMM不会无条件转发这个输出给客户端。
    - Primary的VMM会等到之前的Log条目都被Backup虚机确认收到了才将输出转发给客户端。所以，包含了客户端输入的Log条目，会从Primary的VMM送到Backup的VMM，Backup的VMM不用等到Backup虚机实际执行这个输入，就会发送一个表明收到了这条Log的ACK报文给Primary的VMM。当Primary的VMM收到了这个ACK，才会将Primary虚机生成的输出转发到网络中。
- test-and-set服务
  - 一个节点来提供分布式环境下的原子操作.
  - 如果存在多节点就会出现问题
- majority vote
  - 设置奇数个 (2n + 1)个服务器 来避免脑裂问题. 任何重要决策都需要至少n + 1个服务器同意.
  - 即便存在一次脑裂, 至少有一边会具有n + 1个服务器.
  - 即使脑裂导致的分群发生了变化, 之前的majority 群和之后的必然至少共有一台服务器. 也就是说, 通过这台共有服务器, 即使leader被重新选举, 所有操作的log还是可以被保留.
- 选举
  - 分布式系统中, leader不是必须的, 但却可以大大提高效率
  - leader选举发生在某个节点心跳超时的时刻. 此时这个节点会发起投票.
  - 当然有可能多个节点同时超时, 同时发起投票. 为了避免这类情况频繁出现, 每次重置计时器时, 对于心跳超时时间做一个随机化即可.
    - 但是, 尽管随机化了, 还是要保证心跳超时时间不应该小于或过于接近心跳间隔. 可以设置下限为3个心跳间隔.
    - 超时时间的上限也不应过大, 因为选举期间, 系统是不响应外界请求的.
    - 不同节点的超时时间差必须足够长, 使得第一个选举的节点能够完成一轮选举. 时间差的下限是一条rpc的rtt.



## 总体设计

- raft是应用层之下的一层. 所有的网络请求应要经过raft.
- 在外部输入到来时, raft层先拿到输入的command, 并且同步到过半节点, ack之后leader才commit command, 让应用程序响应. 
- 此外, leader在commit command后, 也需要通知peer.
  - 但是, 这个通知消息在raft中没有被明确定义
  - 一般认为, 这个通知被夹带在下一次的append entry rpc中.
  - 具体来说, leader在心跳或是收到新的客户端请求发送同步要求时, leader的请求中会带有更大的commit号. peer收到后就知道, 可以将之前的操作commit了.
  - 其他副本commit command的操作的响应速度不是特别重要, 因为没有人在等, 不影响客户端的时间延迟.
- 代码实现
  - 具体来说, 这是通过客户端软件在收到请求时, 转发请求到raft实现的.
  - 应用层和raft之间有两个接口
    - Start: raft接收客户端请求. 将请求存到log中. (但raft在执行时不会等到存完(commited)才返回, 只是返回一些信息)
      - raft返回时让客户端知道请求在log中的位置(index)以及当前的任期号 (term number). (这两者组合构成一个唯一标识)
    - ApplyCh: raft层通过这个接口发送applyMsg (command, index)通知客户端, 某个请求已经commit. 
      - 客户端拿到请求commit的消息后, 即可定位到请求, 然后真正执行请求.
      - 所有peer都应该接收到这个applyMsg消息. 不过, 对于peer来说, index意义不大.



## Leader Election

- 启动Raft.

  - (2D) 启动时, 应当读取persisted state, 从而得知

    - `votedFor`: 当前leader (可能为null)
    - `currentTerm`: 当前term (初始化为0)
    - `log[]` 已经被持久化的log

  - (2A) 然后, 需要启动心跳超时计时器. 一旦超时就触发选举.

  - (2A) 选举 - candidate行为

    - 一个follower 增加其current term, 转换为candidate

    - 它给自己投票, 并向其他节点并发调用RequestVote

    - 投票有几种结果

      - candidate拿到了半数以上的vote. 此时candidate转换为leader并且开始向其他节点发送heartbeat

      - candidate在结束前可能收到另一个candidate的appendEntry. 
        - 如果另一个candidate的term >= 当前candidate的term, 则认为另一个candidate是真正的leader, 当前candidate回到follower状态.
        - 否则, 拒绝该rpc 并保持candidate状态.
      - 一段时间过后, 选举没有产生胜者. (因为多个candidate竞争, 票数被分散).
        - 这时不做特殊处理, 直接等待timeout, 让下一次选举发生.

  - (2A) 选举 - follower行为

    - 一个follower收到投票请求时, 会按照一定规则给出true or false.
      - 如果candidate发来的term比自身currentTerm还要小, 拒绝.
      - 如果还没有投票过, 或者之前投的就是这个candidate, 以及candidate的log起码和自身同步(或靠前), 则可以投票
        - 起码同步(at least up-to-date)的准确定义: `arg.lastLogTerm > rf.log.lastTerm`
    - 在一个term中, 一个follower只能vote一次. 
      - term初始化时, votedFor被重置为null (-1).
  
  - (2A) 选举得出的leader会发送心跳给其他follower. (通过appendEntries)



- candidate / leader发现自己的term比其他人小时, 立马转换为follower, 并更新currentTerm
- server在收到来自过去term的请求时, 直接忽略请求.
- 一个server在向其他多个节点发送请求时, 会并发发送
