本研究探究了Apache Hadoop MapReduce框架中Reduce任务的启动时机对作业性能的影响。通过设计和执行一系列对比实验，即设计了同种任务（WordCount）在不同数据规模（100MB、500MB、1GB）下，调整Reduce任务启动的比例参数（SlowStart: 0.2、0.5、0.8、1.0），系统地分析了作业执行时间、资源利用率等关键性能指标。实验结果表明，**Reduce任务启动时机的选择对MapReduce作业性能有显著影响**，不同的应用场景需要采用不同的启动策略以获得最优性能。

---

## 1. 引言
### 1.1 研究背景
Apache Hadoop是当前最流行的分布式计算框架之一，MapReduce是其核心的编程模型。在MapReduce作业中：

+ **Map阶段**处理输入数据，产生中间结果
+ **Shuffle阶段**负责数据的传输、排序和合并
+ **Reduce阶段**对中间结果进行聚合

传统的MapReduce实现中，Reduce任务通常等到**所有Map任务完成后**才开始执行。然而，现代的Hadoop版本（YARN）支持**提前启动Reduce任务**，使其在Map任务执行过程中就开始进行Shuffle和Sort操作。

### 1.2 研究意义
**问题所在：**

+ 当前没有明确的指导原则来确定在何种场景下应该采用哪种Reduce启动策略
+ 过早启动Reduce任务可能导致资源竞争和Map任务性能下降
+ 延迟启动Reduce任务可能导致集群资源利用不充分

**研究价值：**

+ 为MapReduce应用的性能优化提供数据支撑
+ 帮助用户根据应用特征选择合适的启动策略
+ 揭示Reduce启动时机与资源利用之间的内在关系

### 1.3 研究目的
**本研究目的为：** 探究MapReduce中Reduce任务的启动时机及其对作业性能的影响。

---

## 2. 研究内容
本研究主要围绕MapReduce的Reduce任务启动策略展开，在**研究目的**中已明确。具体研究内容包括：

#### 2.1 Reduce任务启动时机分析
Reduce任务的启动时机由参数 `mapreduce.job.reduce.slowstart.completedmaps` 控制。我们将分析此参数在不同取值下，MapReduce作业执行流程的变化以及可能带来的影响。

#### 2.2 性能指标定义
为量化评估不同启动策略的效果，本研究定义并测量以下关键性能指标：

+ **作业完成时间**： 从整个 MapReduce 任务开始，到最后一个 Reduce 任务完成为止的总耗时。  
+ **Map/Reduce平均完成时间**： 
    - 平均 Map 任务完成时间：所有 Map 任务的执行耗时的平均值。
    - 平均 Reduce 任务完成时间：所有 Reduce 任务的执行耗时的平均值。

可用于分析任务的负载是否均衡、是否存在性能瓶颈，也能反映调度策略是否合理。不同启动策略可能影响 		Reduce 阶段的整体耗时。  

+ **Shuffle+Sort耗时**： 指从 Map 输出数据开始传输给 Reduce 任务（Shuffle），并对数据进行排序（Sort）的总耗时。  
+ **CPU利用率**： MapReduce 作业运行期间，系统 CPU 的使用率  
+ **内存利用率**：作业执行过程中内存的使用百分比。

#### 2.3 实验设计
我们将采用多因素对比实验设计，涉及三个主要维度：工作负载类型（WordCount）、数据规模（100MB、500MB、1GB）和Reduce启动比例（SlowStart: 0.2、0.5、0.8、1.0）。通过系统地组合这些维度，单个实验运行多次取性能均值，在多实验下继续对比完成性能分析。

#### 2.4 实验结果与分析
对收集到的MapReduce日志、CPU性能日志进行数据提取、统计分析，通过图表和表格直观呈现结果。分析Reduce启动时机对MapReduce任务性能、CPU利用率、内存占用率的影响，并对不同工作负载和数据规模下的性能差异进行深入探讨。

#### 2.5 结论与实践建议
根据实验结果，总结不同Reduce启动策略的优劣，并针对不同应用场景（如延迟、吞吐、资源等）提出具体的实践优化建议和配置指南。

---

## 3. 实验设置
### 3.1 实验环境
#### 3.1.1 硬件配置
本实验在模拟的分布式集群环境下进行，共 **4个节点**：1个Master节点和3个Worker节点。

| 配置项 | Master节点 | Worker节点 (3个) |
| --- | --- | --- |
| **CPU核数** | 2核  | 2核  |
| **内存大小** | 10GB DDR4 | 10GB DDR4 |
| **网络带宽** | 300Mbps | 300Mbps |
| **存储类型** |  VMDK 虚拟磁盘（SCSI）  40G | VMDK 虚拟磁盘（SCSI）  40G |


#### 3.1.2 软件配置
集群的操作系统和核心软件栈配置如下：

| 软件 | 版本 | 配置说明 |
| --- | --- | --- |
| **操作系统** | Ubuntu20.04、Ubuntu22.04、CentOS7 | 64位，虚拟机 |
| **JDK 版本** | OpenJDK 1.8.0 | 所有节点统一配置，各版本号略有不同 |
| **Hadoop 版本** | Apache Hadoop 3.3.4 | YARN (MRv2) 模式 |
| **Python 版本** | 3.13.1 | 用于数据集生成和数据分析脚本 |


### 3.2 实验负载
#### 3.2.1 数据集
本实验使用wiki上下载的数据集enwiki-latest-pages-articles.xml.bz2的切片。

+ **内容特征**：数据集是维基百科英文版 (English Wikipedia) 的最新完整数据转储 (dump)，具体包含的是文章页面，是大数据集。
+ **数据集规模**：100MB+500MB+1GB
+ **存储**：所有数据集已上传至HDFS。

#### 3.2.2 工作负载
本实验采用了典型的MapReduce工作负载：

+ **WordCount**：
    - **特点**：Map阶段进行词频计数，Reduce阶段进行聚合。Map和Reduce阶段的计算量相对平衡，Shuffle的数据量较大。
    - **代表性**：是MapReduce的“Hello World”程序，常用于评估通用批处理性能。

### 3.3 实验步骤
本实验的执行流程严格遵循预设的步骤，确保实验的可重复性和数据准确性。

**步骤1：集群环境初始化与验证**

1. **验证网络环境**：Master节点通过SSH连接到其余Worker节点。

![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764321433822-a88ef07f-a165-4ae5-bbe6-f63a2ed5f240.png)

2. **启动Hadoop集群**：执行`start-dfs.sh`和`start-yarn.sh`命令启动HDFS和YARN服务。

![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764321273507-8b45f0f5-87ef-4d41-b94a-23f82554c12c.png)

3. 验证集群状态：
    - 通过`jps`命令检查所有节点上的JVM进程，确保`NameNode`、`ResourceManager`、`DataNode`、`NodeManager`等核心进程均正常运行。
    - **【Master节点jps进程信息】**
    - ![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764321323601-3e379362-6792-4c38-af76-58a46e78232d.png)
    - **【各Worker节点JPS进程信息】**
    - ![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764321464983-f3abc1a1-c004-4ee5-974c-7333eb43d8ff.png)
    - ![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764321798379-5798f2b7-8de1-487b-8d65-3f7766869c3b.png)
    - ![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764321825635-c0bee17b-716c-4987-8608-7bf0f334ccc1.png)
    - **【hdfs dfsadmin -report指令****<font style="color:rgba(0, 0, 0, 0.95) !important;">查看集群的整体状态和详细资源信息</font>****】**
    - ![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764321694368-28d617c7-dd10-468f-bc92-e33403eca755.png)
    - ![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764321709742-0db09de9-fbeb-477b-8020-547c591ba2f8.png)
    - ![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764321959925-e9a22861-8420-49a4-be83-47a2d68fcb4f.png)
    - ![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764321975800-95fba794-359b-4854-8920-b0fde574a72c.png)
4. **HDFS空间检查**：执行`hdfs dfs -df -h`确认HDFS有足够的存储空间。

![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764322037520-126b1f23-37d9-4849-ae45-0651bec30590.png)

**步骤2：数据集准备**

1. **原始数据集准备**：在wiki上下载的数据集enwiki-latest-pages-articles.xml.bz2的切片，切片大小包括100MB、500MB、1GB的文本文件。

![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764322184759-b28c1a2a-93f4-4f7f-860e-201f830d56c1.png)

2. **上传至HDFS**：将生成的原始数据集从本地文件系统上传到HDFS的`/user/hadoop/input`目录下，例如：`hdfs dfs -put /path/to/local/1GB_data.txt /user/hadoop/input/`。<font style="background-color:#FBDE28;"></font>![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764322073506-03a12924-4266-4aeb-a0b8-58b82f710c0d.png)
3. **验证HDFS文件**：执行`hdfs dfs -ls /user/hadoop/input`确认文件已成功上传。<font style="background-color:#FBDE28;"></font>

![](https://cdn.nlark.com/yuque/0/2025/png/63105379/1764322245312-a361007a-4666-4ffe-86ea-b97dfa0327bc.png)

**步骤4：实验代码构建**

**实验代码如下：**

[https://github.com/Littleleii/MapReduce511/tree/main/wheel](https://github.com/Littleleii/MapReduce511/tree/main/wheel)

 本实验为了系统地评估 Hadoop MapReduce 在不同 `mapreduce.job.reduce.slowstart.completedmaps` 参数下的性能表现，设计并实现了三个自动化脚本：monitor_real.sh、run_mr_real.sh 和 run_batch.sh。这三个脚本协同工作，实现了集群性能监控、MapReduce 任务自动执行、多组 slowstart 参数批量测试、日志自动归档等完整流程。  

**（1）monitor_real.sh：集群实时性能监控脚本**

`monitor_real.sh` 负责在 MapReduce 作业运行期间，**实时监控每个 Worker 节点的 CPU 利用率和内存占用情况**，并将数据记录到指定日志文件。实验需要在作业运行时记录集群资源消耗，Yarn 自身监控信息不足以满足精度要求、Hadoop Web UI 不便于保存、分析。脚本每隔 1 秒采样一次，包括：CPU 利用率、内存占用率、各节点的监控分隔记录、作业总运行时间

  	节点列表在脚本开头定义：

```plain
NODES=("worker1-zzh" "worker2-zrt" "worker3-haz")
```

脚本通过 SSH 登录 worker 节点读取：

+ `/proc/stat` —— 用于计算 CPU 使用率（两次采样差分法）
+ `/proc/meminfo` —— 用于计算内存使用百分比

该脚本由 `run_mr_real.sh` 自动启动，当 Yarn 检测到 MapReduce 作业结束后自动停止，监控数据写入对应时间戳目录中的 `monitor.log`。

---

**（2）run_mr_real.sh：单次 MapReduce 运行 + 性能监控脚本**

`run_mr_real.sh` 负责执行单次 MapReduce wordcount 作业，并在此过程中：自动对 HDFS 同名输出目录进行删除（避免报错）、自动启动监控脚本 monitor_real.sh、记录 MapReduce 作业的日志输出、将所有日志保存到对应的时间戳文件夹中。

本脚本接收 3 个参数：

```plain
./run_mr_real.sh <input_path> <output_path> <slowstart>
```

本脚本使用外部传入的环境变量 `RUN_LOG_DIR` 创建日志目录，并启动监控脚本：

```plain
monitor_real.sh → monitor.log
```

同时删除HDFS的旧输出：

```plain
hdfs dfs -rm -r -f "$OUTPUT"
```

运行 WordCount，并指定 slowstart 参数：

```plain
-D mapreduce.job.reduce.slowstart.completedmaps=$SLOWSTART
```

MapReduce 执行结果写入：

```plain
job_output.log
```

每次运行会生成一个独立目录，包含：

+ `monitor.log` —— 节点 CPU/MEM 时序数据
+ `job_output.log` —— MapReduce 作业日志（包括提交、运行、结束信息）

---

**（3） run_batch.sh：多 slowstart 参数批处理脚本**

`run_batch.sh` 控制整个实验流程，是本次 MapReduce 性能测试的总控脚本。它实现对不同 slowstart（0.2 / 0.5 / 0.8 / 1.0）自动执行 MapReduce，每种 slowstart 运行 3 次自动创建结构化日志目录，将每一次运行的结果保存到独立时间戳文件夹中

**s**lowstart 参数列表：

```plain
SLOWSTART_VALUES=(0.2 0.5 0.8 1.0)
```

每个 slowstart 运行次数

```plain
RUNS_PER_SS=3
```

**步骤5：数据处理文件**

**（1）common_utils.py：用于数据分析的工具库，解析日志文件，处理多轮实验等，供分析函数调用。**

**（2）analyze_all_metrices.py：用于生成各指标图表。**

**（3）analyze_cpu_slowstart.py：用于生成cpu利用率曲线。**

**（4）analyze_mem_slowstart.py：用于生成内存利用率曲线。**

---

## 4. 实验结果与分析
### 4.1 基于 Enwiki 100MB 数据集的MapReduce性能实验  
#### 4.1.1 任务整体性能分析  
本实验在 100MB 数据集上测试了不同 slowstart（SS）参数对 MapReduce 任务性能的影响，分别从任务总耗时、CPU 利用率、Map 阶段时间、Shuffle 阶段时间、Reduce 阶段时间以及 Shuffle 重叠比例六个维度进行评估。实验结果表明，**slowstart = 0.8** 在多数关键指标上表现最优，是本数据量下的最佳配置。

###### <font style="color:rgb(51, 51, 51);">指标一：任务总耗时</font>
首先，从 任务总耗时来看，SS=0.8 达到最短的 133s，而 SS=1.0 最长达 153s。这说明当 Reduce 在 Map 完成 80% 左右就开始执行时，能够实现更好的阶段重叠，从而缩短整体执行时间。对应地，集群平均 CPU 利用率 在 SS=0.8 时也达到最高（58.98%），说明该配置充分发挥了集群的并行能力，使计算和数据传输更均衡。

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">100MB</font> | <font style="color:rgb(51, 51, 51);">134.33</font> | <font style="color:rgb(51, 51, 51);">140.33</font> | **<font style="color:rgb(51, 51, 51);">133.00</font>** | <font style="color:rgb(51, 51, 51);">153.33</font> | <font style="color:rgb(51, 51, 51);">0.8</font> |


**表 4-1 不同 SlowStart 参数下的任务总耗时 ** 

###### <font style="color:rgb(51, 51, 51);">指标二：Map 阶段耗时 </font>
在 Map 阶段耗时 中，SS=0.8 同样是最短（130.33s），表明适度提前启动的 Reduce 阶段并未对 Map 阶段造成过多资源争夺，反而使整体资源利用更加平稳。

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">100MB</font> | <font style="color:rgb(51, 51, 51);">131.00</font> | <font style="color:rgb(51, 51, 51);">135.33</font> | **<font style="color:rgb(51, 51, 51);">130.33</font>** | <font style="color:rgb(51, 51, 51);">142.67</font> | <font style="color:rgb(51, 51, 51);">0.8</font> |


** 表 4-2 不同 SlowStart 参数下的 Map 阶段耗时 ** 



###### <font style="color:rgb(51, 51, 51);">指标三：Shuffle 阶段耗时 </font>
尽管 **Shuffle 阶段耗时在 SS=1.0 时为 0s**，看似最优，但这是由于 Reduce 只有在全部 Map 完成后才开始执行，Shuffle 与 Map 完全无重叠，导致 Shuffle 时间被计为 0。然而这实际上破坏了整个 pipeline 的效率，使总耗时反而更长。因此该现象属于指标统计方式导致的“伪优势”，并不代表实际性能更好。

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">100MB</font> | <font style="color:rgb(51, 51, 51);">82.33</font> | <font style="color:rgb(51, 51, 51);">43.00</font> | <font style="color:rgb(51, 51, 51);">21.00</font> | **<font style="color:rgb(51, 51, 51);">0.00</font>** | <font style="color:rgb(51, 51, 51);">1.0</font> |


 **表 4-4 不同 SlowStart 参数下的 Reduce 阶段耗时** 

###### <font style="color:rgb(51, 51, 51);">指标四：Shuffle 重叠比例 </font>
 从图中 100MB 数据集的实验结果来看，Shuffle 重叠比例在不同 slowstart（SS）设置下差异明显：SS=0.2 时为 95.93%，SS=0.5 时下降到 82.27%，SS=0.8 回升到 87.22%，而 SS=1.0 则为 0.00%，说明当 Reduce 完全等到 Map 结束（SS=1.0）才启动时，Shuffle 与 Map 阶段完全无法重叠。整体上看，SS=0.2、0.5、0.8 都能实现较高的 Shuffle-Map 重叠，因此被判定为 “Best SS” 区间；但三者中 SS=0.2 的重叠比例最高，表明 Reduce 提前启动越多，越能与 Map 阶段形成 pipeline 并行。与此同时，SS=0.5 和 SS=0.8 的重叠比例虽略低，但仍保持在高位，说明它们在减少等待与维持并行之间可能存在更平衡的启动点；而 SS=1.0 的零重叠直接导致阶段串行化，是最不利于整体性能的设置。  

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">100MB</font> | **<font style="color:rgb(51, 51, 51);">95.93</font>** | <font style="color:rgb(51, 51, 51);">82.27</font> | <font style="color:rgb(51, 51, 51);">87.22</font> | <font style="color:rgb(51, 51, 51);">0.00</font> | <font style="color:rgb(51, 51, 51);">0.2, 0.5, 0.8</font> |


** 表 4-5 不同 SlowStart 参数下的 Shuffle–Map 重叠比例** 

#### 4.1.2 CPU性能分析
##### CPU利用率
###### <font style="color:rgb(51, 51, 51);">指标一：集群平均 CPU 利用率 </font>
CPU 利用率反映了集群资源在 Map、Shuffle、Reduce 三阶段中的活跃程度。在本实验中，SS=0.8 获得最高的 CPU 平均利用率（58.98%），显著高于其他配置。SS=0.2 与 SS=0.5 的利用率分别为 57.02% 和 54.25%，说明 Reduce 过早启动导致长时间等待，使 CPU 的有效计算比例下降。  

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">100MB</font> | <font style="color:rgb(51, 51, 51);">57.02</font> | <font style="color:rgb(51, 51, 51);">54.25</font> | **<font style="color:rgb(51, 51, 51);">58.98</font>** | <font style="color:rgb(51, 51, 51);">54.36</font> | <font style="color:rgb(51, 51, 51);">0.8</font> |


**表 4-6 不同 SlowStart 参数下的****<font style="color:rgb(51, 51, 51);">集群平均 CPU 利用率 </font>**

###### 指标二：集群内存占用
 在 100MB 数据集下，不同 SlowStart 设置的集群平均内存利用率整体维持在 20%–40% 之间，四条曲线几乎重合，说明 Reduce 启动时机对内存占用影响很弱。内存利用率在 Map 启动初期与 Shuffle/Reduce 触发阶段出现轻微峰值，但峰值大小各策略差异不明显。相比之下，SS=1.0 的曲线收尾更晚，体现阶段串行导致的作业拖尾，而非内存负载上升。总体表明：小规模数据下，内存指标对启动策略区分度有限  

![](https://cdn.nlark.com/yuque/0/2025/png/63078543/1764341467306-3154b175-413f-40e2-8a32-3fa06d389ad3.png)

###### 指标三：集群CPU占用
 在 100MB 数据集下，四种 SlowStart 策略在作业前半段（0–7s）CPU 利用率均快速升至 70%–75% 并保持稳定，说明该阶段主要由 Map 并行计算主导，启动策略影响有限。7s 后 CPU 曲线开始分化：SS=0.2 与 SS=0.8 能更长时间维持高负载，而 SS=1.0 提前下滑并在尾部出现拖尾与末端反弹，反映 Reduce 过晚启动导致 pipeline 空洞和阶段串行化。整体表明：在小规模数据下，合理提前 Reduce 启动可以提升 CPU 利用率的连续性并缩短作业尾部时间，而 SS=1.0 是最不利于资源利用与作业完成时间的设置。  

![](https://cdn.nlark.com/yuque/0/2025/png/63078543/1764343578237-879391c5-88c8-4097-9c1d-83453feba651.png)

#### 4.1.3 Slowstart 参数对性能的影响总结  
<font style="color:rgba(6, 8, 31, 0.88);">SlowStart = 0.8 在“并行计算”和“资源争抢”之间找到了最佳平衡点。</font>

<font style="color:rgba(6, 8, 31, 0.88);">在Reduce 阶段在 Map 快结束时（完成 80%）提前启动 Shuffle，既没有像SlowStart = 0.2因为过早启动导致网络拥堵拖慢 Map，也没有如SlowStart = 1.0因为过晚启动导致 Map 做完后 Reduce 才开始冷启动，浪费了时间。</font>

综上，**用于本数据规模（100MB）时，slowstart=0.8 在计算资源利用、阶段重叠、任务时延方面达到了最优平衡，是整体性能最好的配置**。这一结果也说明，适度的 Reduce 提前启动有助于充分利用集群资源，而过早或过晚都会引发阶段等待、资源浪费或 pipeline 中断，从而降低性能。

### 4.2 基于 Enwiki 500MB 数据集的MapReduce性能实验  
#### <font style="color:rgb(51, 51, 51);">4.2.1 任务整体性能分析  </font>
###### <font style="color:rgb(51, 51, 51);">指标一：任务总耗时</font>
在 500MB 数据规模下，slowstart = 0.2 的任务总耗时最短（546s），略优于 SS=0.8（550.33s）。说明在较大数据量下，Reduce 提前启动带来的等待开销相对变小，反而能够更早利用网络带宽进行 Shuffle，缩短整体执行时间。  

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">500MB</font> | **<font style="color:rgb(51, 51, 51);">546.00</font>** | <font style="color:rgb(51, 51, 51);">567.33</font> | <font style="color:rgb(51, 51, 51);">550.33</font> | <font style="color:rgb(51, 51, 51);">569.33</font> | <font style="color:rgb(51, 51, 51);">0.2</font> |


**表 4-7 不同 SlowStart 参数下的****<font style="color:rgb(51, 51, 51);">任务总耗时 </font>**

###### <font style="color:rgb(51, 51, 51);">指标二：Map 阶段耗时 </font>
SS=0.2 的 Map 阶段耗时最短。由于 Reduce 提前启动分担了部分网络传输，减轻了 Map 后期的网络压力，使 Map 执行更流畅，因此 0.2 在大数据量下对 Map 性能更友好。

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">500MB</font> | **<font style="color:rgb(51, 51, 51);">526.00</font>** | <font style="color:rgb(51, 51, 51);">553.00</font> | <font style="color:rgb(51, 51, 51);">539.33</font> | <font style="color:rgb(51, 51, 51);">527.33</font> | <font style="color:rgb(51, 51, 51);">0.2</font> |


**表 4-8 不同 SlowStart 参数下的Map 阶段耗时 **

###### <font style="color:rgb(51, 51, 51);">指标三：Shuffle 阶段耗时 </font>
SS=1.0 的 Shuffle 阶段耗时为 0，看似最优，但这是 **指标统计假象**：SS=1.0 会 **等待所有 Map 完成后** 才启动 Reduce，Shuffle 与 Map 完全无重叠，导致 Shuffle 时间被统计为 0，实际上 **总耗时却是最差的（569s），**因此，**Shuffle=0 不代表性能最好**。

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">500MB</font> | <font style="color:rgb(51, 51, 51);">649.00</font> | <font style="color:rgb(51, 51, 51);">406.00</font> | <font style="color:rgb(51, 51, 51);">163.00</font> | **<font style="color:rgb(51, 51, 51);">30.00</font>** | <font style="color:rgb(51, 51, 51);">1.0</font> |


**表 4-9 不同 SlowStart 参数下的Shuffle 阶段耗时**

###### <font style="color:rgb(51, 51, 51);">指标四：Shuffle 重叠比例 </font>
从图中 500MB 数据集的实验结果来看，Shuffle 重叠比例在不同 slowstart（SS）设置下呈现出“高重叠集中在提前启动、SS=1.0 为零”的特征：SS=0.2 时为 95.08%，SS=0.5 几乎相同为 95.03%，SS=0.8 略低为 90.31%，而 SS=1.0 仍为 0.00%。这说明在 500MB 规模下，只要 Reduce 不是等到所有 Map 完成后再启动（即 SS<1.0），Shuffle 阶段基本都能与 Map 产生大幅重叠，因此 SS=0.2、0.5、0.8 都被列为 Best SS 区间。

但值得注意的是，尽管 SS=0.2 和 SS=0.5 的重叠比例几乎一致且最高，SS=0.8 的重叠比例略低却仍保持在 90% 以上，表明三者在该数据规模下都能形成较充分的 pipeline 并行；真正拉开性能差距的关键通常不只是“能不能重叠”，而是 Reduce 启动是否过早导致等待、或过晚导致并行不足。相比之下，SS=1.0 仍然完全失去 Shuffle 重叠，使 Map 与 Reduce 串行执行，是最不利于整体效率的设置。

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">500MB</font> | <font style="color:rgb(51, 51, 51);">95.08</font> | <font style="color:rgb(51, 51, 51);">95.03</font> | <font style="color:rgb(51, 51, 51);">90.31</font> | <font style="color:rgb(51, 51, 51);">0.00</font> | <font style="color:rgb(51, 51, 51);">0.2,0.5,0.8</font> |


**表 4-10 不同 SlowStart 参数下的****<font style="color:rgb(51, 51, 51);">Shuffle 重叠比例 </font>**

#### 4.2.2CPU性能分析
###### <font style="color:rgb(51, 51, 51);">指标一：集群平均 CPU 利用率</font>
| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">500MB</font> | <font style="color:rgb(51, 51, 51);">65.92</font> | **<font style="color:rgb(51, 51, 51);">66.81</font>** | <font style="color:rgb(51, 51, 51);">65.24</font> | <font style="color:rgb(51, 51, 51);">62.57</font> | <font style="color:rgb(51, 51, 51);">0.5</font> |


**表 4-11 不同 SlowStart 参数下的****<font style="color:rgb(51, 51, 51);">集群平均 CPU 利用率</font>**

###### 指标二：集群平均内存占用
 在 500MB 数据集下，不同 SlowStart 设置的集群平均内存利用率整体维持在 30%–40% 之间。除作业起步阶段（0–5s）因 Map 扩展与中间结果缓存而出现短暂抬升外，5s 之后四条曲线进入长时间稳定平台期，并且在全程几乎重合，仅有极小幅度差异。这说明在该规模下 Reduce 启动时机对“平均内存占用”影响不显著，内存仍非主要瓶颈  

![](https://cdn.nlark.com/yuque/0/2025/png/63078543/1764341491454-bdd46497-128d-4fe4-9348-cb6ca780e2b7.png)

###### 指标三：集群CPU占用
 在 500MB 数据集下，四种 SlowStart 策略的 CPU 利用率在作业主体阶段（约 2–40s）均稳定维持在 70%–73% 的高位平台，且曲线几乎完全重合，说明该规模下 Map 与 Shuffle/Reduce 的流水线足够饱满，不同 Reduce 启动时机对主干计算阶段影响较小。策略差异主要集中在尾部收尾阶段：SS=1.0 下降滞后且更陡，反映 Reduce 过晚启动导致后期集中收尾；而 SS=0.2 与 SS=0.8 尾部更平滑，表现出更连续的 pipeline 完成特征。  

![](https://cdn.nlark.com/yuque/0/2025/png/63078543/1764343619324-fe6d1c5b-0f2e-4a80-bea5-e19289883cc5.png)

#### **4.2.3 Slowstart 参数对性能的影响总结  **
综合来看：500MB 数据规模下，slowstart=0.2 更偏“总体性能最优”，适合追求最短任务总耗。slowstart=0.5 更适合集群资源利用最大化；slowstart=0.8 则在 Reduce 阶段表现最佳。比起 100MB 数据集的 “0.8 最优”，500MB 更适合提前启动 Reduce（SS=0.2 或 0.5），说明数据规模变大后 Shuffle 带宽占比提升，Reduce 提前启动反而更有利于缓存网络压力。

### 4.3 基于 Enwiki 1G 数据集的MapReduce性能实验  
#### 4.3.1 基于 Enwiki 1G 数据集的MapReduce性能实验
######  指标一：任务总耗时  
1G 数据量下，slowstart = 0.5 的任务总耗时最短（851s），优于 SS=0.2 和 SS=0.8。  
说明在更大规模的数据处理中，需要适度延迟 Reduce 的启动，使 Map 阶段完成度达到一定比例后再进入 Shuffle–Reduce 才能优化整体流水线效率。  

**<font style="color:rgb(51, 51, 51);">优化目标</font>**<font style="color:rgb(51, 51, 51);">: 数值越小越优</font>

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">1G</font> | <font style="color:rgb(51, 51, 51);">860.00</font> | **<font style="color:rgb(51, 51, 51);">851.00</font>** | <font style="color:rgb(51, 51, 51);">860.00</font> | <font style="color:rgb(51, 51, 51);">927.00</font> | <font style="color:rgb(51, 51, 51);">0.5</font> |


**表 4-12 1G数据集下不同 SlowStart 参数下的****<font style="color:rgb(51, 51, 51);">集群任务总耗时  </font>**

######  指标二：Map 阶段耗时
Map 阶段耗时在 SS=0.5 和 SS=0.8 下均最短（833s）。  
这说明在 1G 数据规模下，Reduce 不宜启动得过早，否则会造成网络争抢，从而拉长 Map 阶段执行时间。适度推迟 Reduce 更有利于确保 Map 吞吐的稳定性。  

**<font style="color:rgb(51, 51, 51);">优化目标</font>**<font style="color:rgb(51, 51, 51);">: 数值越小越优</font>

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">1G</font> | <font style="color:rgb(51, 51, 51);">843.00</font> | **<font style="color:rgb(51, 51, 51);">833.00</font>** | **<font style="color:rgb(51, 51, 51);">833.00</font>** | <font style="color:rgb(51, 51, 51);">864.00</font> | <font style="color:rgb(51, 51, 51);">0.5, 0.8</font> |


**表 4-13 1G数据集下不同 SlowStart 参数下的****<font style="color:rgb(51, 51, 51);">集群 Map 阶段耗时</font>**



######  指标三：Shuffle 阶段耗时  
SS=1.0 的 Shuffle 耗时为 0，这是由于：

+ Reduce 只有在所有 Map 完成后才启动
+ Shuffle 与 Map 完全无重叠，因此不计入 Shuffle 耗时
+ 实际任务总耗时反而最差（927s）

因此 **Shuffle=0 不代表性能更好，而是 slowstart 设置过大导致流水线断裂的表现**。

**<font style="color:rgb(51, 51, 51);">优化目标</font>**<font style="color:rgb(51, 51, 51);">: 数值越小越优</font>

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">1G</font> | <font style="color:rgb(51, 51, 51);">649.00</font> | <font style="color:rgb(51, 51, 51);">406.00</font> | <font style="color:rgb(51, 51, 51);">163.00</font> | **<font style="color:rgb(51, 51, 51);">30.00</font>** | <font style="color:rgb(51, 51, 51);">1.0</font> |


**表 4-14 1G数据集下不同 SlowStart 参数下的****<font style="color:rgb(51, 51, 51);">集群 Shuffle 阶段耗时</font>**

######  指标四：Shuffle 重叠比例  
从图中 1G 数据集的实验结果来看，Shuffle 重叠比例随 slowstart（SS）变化趋势更加明显：SS=0.2 时达到 97.38%，SS=0.5 仍保持很高为 95.57%，但 SS=0.8 明显下降到 83.44%，而 SS=1.0 依旧为 0.00%。这表明在更大规模数据下，Reduce 提前启动（SS=0.2、0.5）依然能够让 Shuffle 与 Map 阶段高度并行；但当启动推迟到 SS=0.8 时，Map 阶段已消耗了更多时间，导致 Shuffle 可重叠的窗口变小，从而出现显著的重叠比例下滑。SS=1.0 则完全失去并行机会，使 Shuffle 和 Reduce 被迫等待 Map 全部结束。

因此，尽管 SS=0.2、0.5、0.8 都被标为 Best SS 区间，但在 1G 场景中，SS=0.2 与 SS=0.5 显然更有利于形成紧凑的 pipeline，提高并行度；而 SS=0.8 的重叠下降提示它可能开始接近“启动偏晚”的临界点。

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">1G</font> | **<font style="color:rgb(51, 51, 51);">97.38</font>** | <font style="color:rgb(51, 51, 51);">95.57</font> | <font style="color:rgb(51, 51, 51);">83.44</font> | <font style="color:rgb(51, 51, 51);">0.00</font> | <font style="color:rgb(51, 51, 51);">0.2, 0.5, 0.8</font> |


**表 4-15 1G数据集下不同 SlowStart 参数下的****<font style="color:rgb(51, 51, 51);">集群Shuffle 重叠比例 </font>**

#### 4.3.2 CPU性能分析
###### <font style="color:rgb(51, 51, 51);">指标一：集群平均 CPU 利用率</font>
 SS=0.2 的 CPU 利用率最高（97.51%），几乎达到满负载状态。  
Reduce 提前启动使 CPU 在 Map 后半段和 Shuffle 阶段始终保持高活跃度，减少空闲时间。这也体现出大规模数据集在 slowstart 较小情况下，资源利用率更佳。  

| **<font style="color:rgb(51, 51, 51);">Dataset</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.2</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.5</font>** | **<font style="color:rgb(51, 51, 51);">SS=0.8</font>** | **<font style="color:rgb(51, 51, 51);">SS=1.0</font>** | **<font style="color:rgb(51, 51, 51);">Best SS</font>** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| <font style="color:rgb(51, 51, 51);">1G</font> | **<font style="color:rgb(51, 51, 51);">97.51</font>** | <font style="color:rgb(51, 51, 51);">94.77</font> | <font style="color:rgb(51, 51, 51);">93.58</font> | <font style="color:rgb(51, 51, 51);">89.29</font> | <font style="color:rgb(51, 51, 51);">0.2</font> |


**表 4-16 1G数据集下不同 SlowStart 参数下的****<font style="color:rgb(51, 51, 51);">集群平均 CPU 利用率</font>**

###### 指标二：集群平均内存占用
 在 1G 数据集下，集群平均内存利用率整体维持在 35%–45% 的中等水平，作业启动初期快速上升到平台区间，随后在 5s–50s 的主体阶段四种 SlowStart 策略曲线高度重合，仅出现更频繁的锯齿状波动，反映更大规模 Shuffle/merge 带来的周期性缓冲涨落。不同策略的差异主要集中在 50s 之后的尾部阶段，表现为内存下降的时刻与持续时长不同  

![](https://cdn.nlark.com/yuque/0/2025/png/63078543/1764341526373-05f5218d-a73c-44f2-b436-12b255effecf.png)

###### 指标三：集群CPU占用
 在 1G 数据集下，CPU 利用率在作业启动后迅速升至接近 100%，并在约 2–50s 的主体阶段持续满载，四种 SlowStart 策略曲线几乎重合，说明在大规模负载下作业主干计算完全饱和，Reduce 启动时机对主体阶段 CPU 利用率影响很弱。策略差异主要体现在尾部收敛阶段：SS=0.2 与 SS=0.5 能更长时间维持高负载、尾部下降更晚，表明提前启动 Reduce 有助于延长 Shuffle 重叠并形成更紧凑的 pipeline；而 SS=0.8 与 SS=1.0 更早进入低负载收尾，反映 Reduce 启动偏晚导致 tail 变长与资源空洞增加。  

![](https://cdn.nlark.com/yuque/0/2025/png/63078543/1764343632378-9c4d4781-2ed2-40cd-a48d-e1b586cce60c.png)

#### 4.3.3 Slowstart 参数对性能的影响总结  
在 1G 数据规模下，slowstart=0.5 在 Map 阶段耗时与任务总耗时方面表现最佳，同时保持较高的 Shuffle–Map 重叠度，是整体性能最优的配置。随着数据规模增大，适度延迟 Reduce 启动可以避免过度的网络争抢，使 Map 阶段更稳定，从而提升整个 MapReduce 流水线的效率。



---

## 5. 结论
第4节对比分析了不同slowstart参数在三种数据规模（100MB、500MB、1GB）下对 MapReduce 各阶段性能、Shuffle 行为、CPU以及内存利用率以及任务总耗时的影响。综合实验结果表明：**slowstart参数对 MapReduce性能有显著的影响，且最优值随数据规模变化而变化**。整体表现呈现出一个清晰规律：  

+ **小数据量（100MB）适合较小的 slowstart**，也即Reduce需要尽早启动，但同时在真实的实验环境下，Map阶段是否稳定、是否均匀、是否受到shuffle抢宽带也会影响作业运行的性能。
+ **中等数据量（500MB）适合中等的slowstart**，平衡 Shuffle–Map 的负载与网络压力。
+ **大数据量（1GB）适合偏晚的slowstart**，既保证Map阶段稳定，又避免Shuffle过早带来的网络拥塞。

实验发现随着数据规模从 100MB 到1GB 逐步增大，最优 SlowStart 发生了明确的变化：

| Dataset | 最优 SS | 主要原因 |
| --- | --- | --- |
| **100MB** | **0.8** | Map 阶段短，资源空闲多，Reduce 早启动可提高流水化效率 |
| **500MB** | **0.2–0.5** | 数据量变大后，提前启动 Reduce 能分担部分网络传输，但过早会与 Map 抢占资源 |
| **1GB** | **0.5** | 数据量大，必须避免 Reduce 过早启动造成网络拥塞，使 Map 阶段变不稳定 |


**结论1：也就是最优SlowStart参数具有数据规模相关性，不存在固定值。**

**对于100MB：提前启动有利**

+ Map 阶段很短，机器空闲资源多。
+ Reduce 早启动可以立刻接收 Shuffle 数据，使 pipeline 更紧凑。
+ 减少 Map→Shuffle→Reduce 间的等待与时延。

 因此较大的 SS（0.8）效果最好。

**对于500MB：过早启动反而造成拥塞**

+ Reduce 若太早启动（SS=0.2），会与 Map 抢网络资源，但此时部分 Map 已完成，可适度提前帮助传输。
+ SS=0.8 太晚，不能利用前期带宽，导致等待增多。

中等数据量下，SS 过大或过小都会降低效率。

**对于1GB：需显著延迟启动**

+ 数据量大，网络拥塞风险高。
+ Reduce 启动过早会强迫 Map 推送大量数据→导致 Map 执行时间变长（网络被抢占）。
+ SS≈0.5 能保证至少一半 Map 完成再启动 Reduce，保证 Map 阶段稳定性。

**结论2：Reduce 的最佳启动时间取决于 Map 阶段的压力与网络负载情况。**

实验显示：

+ **SS=1.0 的 Shuffle 重叠始终为 0（无重叠）**，此时 Reduce 必须等待所有 Map 完成才启动——虽然“统计上 Shuffle 阶段=0”，但实际是**最差性能**（因为等了太久）。
+ **SS=0.2、0.5、0.8 的重叠比例均可达到 100%**，但：
    - 重叠比例高不代表整体性能最好。
    - 例如在 500MB 下 SS=0.5 的重叠比例是 100%，但耗时却比 SS=0.2 更长。

**结论3：Shuffle–Map 重叠比例不能作为判断好性能的核心指标。真正关键的是：Reduce 启动时机是否匹配 Map 阶段的资源压力。**

在三个数据规模下，**最佳 SS 对应的 CPU 利用率也最高**：

    - 100MB：SS=0.8（58.98%）
    - 500MB：SS=0.5（66.81%）
    - 1GB：SS=0.2（97.51%）

**结论4：最佳 SlowStart 实际上是在最大化 CPU 有效利用率，让 Map、Shuffle、Reduce 三段尽可能并行而非互相等待。**



---

## 6. 分工
| 姓名 | 学号 | 具体工作内容 | 贡献度 (%) |
| --- | --- | --- | --- |
| **王茜蕾** | 51285903124 | 集群设计、环境搭建、运行脚本构建、数据分析、报告撰写 | 25% |
| **周珠晗** | 51285903123 | 环境搭建、数据分析、报告撰写 | 25% |
| **张睿桐** | 51285903135 | 环境搭建、数据分析、报告撰写 | 25% |
| **何奡喆** | 51285903132 | 环境搭建、数据分析、图表生成代码构建、报告撰写 | 25% |




## 
