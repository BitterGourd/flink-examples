## Flink Checkpoint

### 一 Checkpoint 执行机制

> 本文摘自以下文章
>
> InfoQ [Apache Flink 进阶（三）：Checkpoint 原理剖析与应用实践](https://www.infoq.cn/article/WkGozMQQExq6Xm5eJl1E)
>
> Ververica [Apache Flink 管理大型状态之增量 Checkpoint 详解](https://ververica.cn/developers/manage-large-state-incremental-checkpoint/)

Flink 中由 JobManager 触发 Checkpoint 操作，下图左侧是 Checkpoint Coordinator (JM 组件)，是整个 Checkpoint 的发起者，中间是由两个 source，一个 sink 组成的 Flink 作业，最右侧的是持久化存储，在大部分用户场景中对应 HDFS。

**1.第一步，Checkpoint Coordinator 向所有 source 节点 trigger Checkpoint**

![Apache Flink进阶（三）：Checkpoint原理剖析与应用实践](https://static001.infoq.cn/resource/image/81/1f/817090dc706a6bc3f97ce2b5045f651f.png)

**2.第二步，source 节点向下游广播 barrier，这个 barrier 就是实现 Chandy-Lamport 分布式快照算法的核心，下游的 task 只有收到所有 input 的 barrier 才会执行相应的 Checkpoint**

![Apache Flink进阶（三）：Checkpoint原理剖析与应用实践](https://static001.infoq.cn/resource/image/e8/1a/e834a9fb615fc44e1d484a41ce858f1a.png)

**3.第三步，当 task 完成 state 备份后，会将备份数据的地址 (state handle) 通知给 Checkpoint coordinator**

![Apache Flink进阶（三）：Checkpoint原理剖析与应用实践](https://static001.infoq.cn/resource/image/38/42/38063dfab59c73f657d9eebef9216d42.png)

**4.第四步，下游的 sink 节点收集齐上游两个 input 的 barrier 之后，会执行本地快照，这里特地展示了 RocksDB incremental Checkpoint 的流程，首先 RocksDB 会全量刷数据到磁盘上（红色大三角表示），然后 Flink 框架会从中选择没有上传的文件进行持久化备份（紫色小三角）**

![Apache Flink进阶（三）：Checkpoint原理剖析与应用实践](https://static001.infoq.cn/resource/image/32/5d/324f573d9f0d66b3544230b0ca639e5d.png)

**5.同样的，sink 节点在完成自己的 Checkpoint 之后，会将 state handle 返回通知 Coordinator**

![Apache Flink进阶（三）：Checkpoint原理剖析与应用实践](https://static001.infoq.cn/resource/image/3b/95/3b0a07e86d16002d35dfe29b55a66c95.png)

**6.最后，当 Checkpoint coordinator 收集齐所有 task 的 state handle，就认为这一次的 Checkpoint 全局完成了，向持久化存储中再备份一个 Checkpoint meta 文件**

![Apache Flink进阶（三）：Checkpoint原理剖析与应用实践](https://static001.infoq.cn/resource/image/22/a7/2213f88974555dcb5e3f21ada20030a7.png)

### 二 RocksDB StateBackend 增量 checkpoint 机制

Flink 的增量 checkpoint 以 RocksDB 的 checkpoint 为基础。RocksDB 是一个 LSM 结构的 KV 数据库，把所有的修改保存在内存的可变缓存中（称为 memtable），所有对 memtable 中 key 的修改，会覆盖之前的 value，当前 memtable 满了之后，RocksDB 会将所有数据以有序的写到磁盘。当 RocksDB 将 memtable 写到磁盘后，整个文件就不再可变，称为有序字符串表（sstable）。

RocksDB 的后台压缩线程会将 sstable 进行合并，就重复的键进行合并，合并后的 sstable 包含所有的键值对，RocksDB 会删除合并前的 sstable。

在这个基础上，Flink 会记录上次 checkpoint 之后所有新生成和删除的 sstable，另外因为 sstable 是不可变的，Flink 用 sstable 来记录状态的变化。为此，**Flink 调用 RocksDB 的 flush，强制将 memtable 的数据全部写到 sstable，并硬链到一个临时目录中。这个步骤是在同步阶段完成，其他剩下的部分都在异步阶段完成，不会阻塞正常的数据处理。**

Flink 将所有新生成的 sstable 备份到持久化存储（比如 HDFS，S3），并在新的 checkpoint 中引用。Flink 并不备份前一个 checkpoint 中已经存在的 sstable，而是引用他们。Flink 还能够保证所有的 checkpoint 都不会引用已经删除的文件，因为 RocksDB 中文件删除是由压缩完成的，压缩后会将原来的内容合并写成一个新的 sstable。因此，Flink 增量 checkpoint 能够切断 checkpoint 历史。

为了追踪 checkpoint 间的差距，备份合并后的 sstable 是一个相对冗余的操作。但是 Flink 会增量的处理，增加的开销通常很小，并且可以保持一个更短的 checkpoint 历史，恢复时从更少的 checkpoint 进行读取文件，因此我们认为这是值得的。

**举个栗子**

![img](https://ververica.cn/wp-content/uploads/2019/09/Checkpoint-.jpg)

上图以一个有状态的算子为例，checkpoint 最多保留 2 个，上图从左到右分别记录每次 checkpoint 时本地的 RocksDB 状态文件，引用的持久化存储上的文件，以及当前 checkpoint 完成后文件的引用计数情况。

- **Checkpoint 1 的时候**，本地 RocksDB 包含两个 sstable 文件，该 checkpoint 会把这两个文件备份到持久化存储，当 checkpoint 完成后，对这两个文件的引用计数进行加 1，引用计数使用键值对的方式保存，其中键由算子的当前并发以及文件名所组成。我们同时会维护一个引用计数中键到对应文件的隐射关系。
- **Checkpoint 2 的时候**，RocksDB 生成两个新的 sstable 文件，并且两个旧的文件还存在。Flink 会把两个新的文件进行备份，然后引用两个旧的文件，当 checkpoint 完成时，Flink 对这 4 个文件都进行引用计数 +1 操作。
- **Checkpoint 3 的时候**，RocksDB 将 sstable-(1)，sstable-(2) 以及 sstable-(3) 合并成 sstable-(1,2,3)，并且删除了三个旧文件，新生成的文件包含了三个删除文件的所有键值对。sstable-(4) 还继续存在，生成一个新的 sstable-(5) 文件。Flink 会将 sstable-(1,2,3) 和 sstable-(5) 备份到持久化存储，然后增加 sstable-4 的引用计数。由于保存的 checkpoint 数达到上限（2 个），因此会删除 checkpoint 1，然后对 checkpoint 1 中引用的所有文件（sstable-(1) 和 sstable-(2)）的引用计数进行 -1 操作。
- **Checkpoint 4 的时候**，RocksDB 将 sstable-(4)，sstable-(5) 以及新生成的 sstable-(6) 合并成一个新的 sstable-(4,5,6)。Flink 将 sstable-(4,5,6) 备份到持久化存储，并对 sstabe-(1,2,3) 和 sstable-(4,5,6) 进行引用计数 +1 操作，然后删除 checkpoint 2，并对 checkpoint 引用的文件进行引用计数 -1 操作。这个时候 sstable-(1)，sstable-(2) 以及 sstable-(3) 的引用计数变为 0，Flink 会从持久化存储删除这三个文件。

**从 checkpoint 恢复以及性能**

开启增量 checkpoint 之后，不需要再进行其他额外的配置。如果 Job 异常，Flink 的 JobMaster 会通知所有 task 从上一个成功的 checkpoint 进行恢复，不管是全量 checkpoint 还是增量 checkpoint。每个 TaskManager 会从持久化存储下载他们需要的状态文件。

尽管增量 checkpoint 能减少大状态下的 checkpoint 时间，但是天下没有免费的午餐，我们需要在其他方面进行舍弃。增量 checkpoint 可以减少 checkpoint 的总时间，但是也可能导致恢复的时候需要更长的时间**。**如果集群的故障频繁，Flink 的 TaskManager 需要从多个 checkpoint 中下载需要的状态文件（这些文件中包含一些已经被删除的状态），作业恢复的整体时间可能比不使用增量 checkpoint 更长。

另外在增量 checkpoint 情况下，我们不能删除旧 checkpoint 生成的文件，因为新的 checkpoint 会继续引用它们，这可能导致需要更多的存储空间，并且恢复的时候可能消耗更多的带宽。

关于控制便捷性与性能之间平衡的策略可以参考此文档：
https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/ops/state/large_state_tuning.html