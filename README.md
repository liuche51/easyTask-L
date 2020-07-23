# easyTask-L

* 一个方便触发一次性或周期性任务执行的分布式架构组件，支持海量,高并发,高可用的任务处理
* A distributed architecture component that facilitates the triggering of one-time or periodic task execution, supporting massive, highly concurrent, and highly available task processing

## 使用场景(Usage scenarios)

* 乘坐网约车结单后30分钟若顾客未评价，则系统将默认提交一条评价信息
* 银行充值接口，要求5分钟后才可以查询到结果成功OR失败
* 会员登录系统30秒后自动发送一条登录短信通知
* 每个登录用户每隔10秒统计一次其某活动中获得的积分

## 特性(Features)

![Architecture](https://images.cnblogs.com/cnblogs_com/liuche/1811577/o_200722062611QQ%E5%9B%BE%E7%89%8720200722142544.png)
* 高可用：因为我们是分布式leader-follow集群，每个任务多有多个备份数据，节点宕机，集群自动重新选举。所以可靠性非常高
* 秒级触发：我们是采用时钟秒级分片的数据结构，支持秒级触发任务。不早也不迟
* 分布式：组件支持分布式
* 高并发：支持多线程同时提交任务，支持多线程同时执行任务
* 数据一致性：使用TCC事务机制，保障数据在集群中的强一致性
* 海量任务：节点可以存储非常多的任务，只要内存和磁盘足够。触发效率也是极高。需要配置好分派任务线程池和执行任务线程池大小即可
* 开源：组件完全在GitHub上开源。任何人都可以随意使用，在不侵犯著作权情况下
* 易使用：无需独立部署集群，嵌入式开发。不过多的依赖于第三方中间件，除了zookeeper。

## 总体架构(Architecture)

![Architecture](https://images.cnblogs.com/cnblogs_com/liuche/1811577/o_200722062629%E5%9B%BE%E7%89%871.png)
　　整体采用分布式设计，leader-follow风格。集群中每一个节点都是leader，同时也可能是其他某个节点的follow。每个leader都有若干个follow。leader上提交的新任务都会强制同步到follow中，删除任务同时也会强制删除follow中的备份任务。集群中所有节点都会在zookeeper中注册并维持心跳。

## 架构之环形队列(AnnularQueue)

![Architecture](https://images.cnblogs.com/cnblogs_com/liuche/1811577/o_200722062635%E5%9B%BE%E7%89%872.png)
　　环形队列在之前单机版的easyTask中也讲过，原理都是类似的。客户端提交任务，服务端先将任务进行持久化，再添加上环形队列这个数据结构中去，等待时间片轮询的到来。不同的是这里的持久化机制，改成了分布式存储了。不仅leader自己存储起来，还要同步存储到其follow中去。删除一个任务也是类似的过程。

　　任务添加时会计算其触发所属的时间分片槽，等环形队列的始终秒针到达时会判断任务是否可以被执行了。如果可以执行了，则分派任务线程池将其丢入执行任务线程池等待执行。只要执行任务线程池线程数足够，任务将立即得到执行。

## 开始入门(Getting started)

* pom添加引用
```xml
<dependency>
    <groupId>com.github.liuche51</groupId>
    <artifactId>easyTask-L</artifactId>
    <version>1.0.1</version>
</dependency>
```
* 定义好您要执行的任务类(Define the task class you want to perform)

　　这个需要根据具体情况，创建你要处理的任务类。任务类都需要继承Task 这个父类以及实现Runnable 的run接口。这里可以写你的任务逻辑，getParam()可以获取到你提交任务时传入的参数。
```java
public class CusTask1 extends Task implements Runnable {
    private static Logger log = LoggerFactory.getLogger(CusTask1.class);

    @Override
    public void run() {
        Map<String, String> param = getParam();
        if (param != null && param.size() > 0)
            log.info("任务1已执行!姓名:{} 生日:{} 年龄:{} 线程ID:{}", param.get("name"), param.get("birthday"), param.get("age"),param.get("threadid"));

    }
}
```
* 简单应用示例代码(Simply apply the sample code)
```java
public class Main {
    public static void main(String[] args){
        try {
            EasyTaskConfig config =new EasyTaskConfig();
            config.setTaskStorePath("C:/db/node1");
            config.setZkAddress("127.0.0.1:2181");
            AnnularQueue annularQueue = AnnularQueue.getInstance();
            annularQueue.start(config);
            CusTask1 task1 = new CusTask1();
            task1.setEndTimestamp(ZonedDateTime.now().plusSeconds(10).toInstant().toEpochMilli());
            Map<String, String> param = new HashMap<String, String>() {
                {
                    put("name", "Lily");
                    put("birthday", "1996-1-1");
                    put("age", "28");
                    put("threadid", String.valueOf(Thread.currentThread().getId()));
                }
            };
            task1.setParam(param);
            AnnularQueue.getInstance().submitAllowWait(task1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```
　　注意：AnnularQueue只需要在你的系统启动时调用一次start方法即可，切勿重复调用。
## 常用接口(API)
　　easyTask-L的API设计比较简洁、易于理解和使用。主要涉及环形队列类、配置类、任务超类以及监控类四个方面。下面逐一做简单介绍。本文只对比较重要的API做介绍，其他API还望读者自行探索
###环形队列(AnnularQueue)
　　环形队列类设计为单例模式。通过AnnularQueue.getInstance();获取。

　　1、void start(EasyTaskConfig config) throws Exception：

　　启动easyTask-L，包括环形队列时钟、集群等一系列工作。且只需要在系统启动时调用一次即可。切勿重复调用。参数config是必须的，系统将此配置参数实例作为当前工作的参数。如果启动失败将抛出异常。

　　2、String submit(Task task) throws Exception：

　　　　向环形队列中提交新任务。提交失败则抛出异常

　　3、String submitAllowWait(Task task) throws Exception

　　　  向环形队列中提交新任务。提交失败则抛出异常。如果环形队列还没有启动成功，则任务不抛出异常，等待启动后再正式提交任务。
###配置类(EasyTaskConfig)
　　前面说了环形队列(集群)启动时需要给一个启动配置参数。因为这是系统必须的东西。

　　1、String zkAddress;

　　集群zookeeper地址配置。必填  如:127.0.0.1:2181

　　2、int backupCount ;

　　任务备份数量，默认2。最大2，超过部分无效

　　3、String taskStorePath;

　　自定义任务本地存储路径。必填

　　4、int sQLlitePoolSize

　　sqlite连接池大小设置。默认cpu数的两倍

　　5、int serverPort

　　设置当前节点Netty服务端口号。默认2020

　　6、int timeOut

　　设置集群通信调用超时时间。默认3秒

　　7、int loseTimeOut

　　ZK节点信息失效超时时间。默认超过60s就判断为失效节点，任何其他节点可删除掉

　　8、int deadTimeOut

　　ZK节点信息死亡超时时间。默认超过30s就判断为Leader失效节点，其Follow节点可进入选举新Leader

　　9、int heartBeat

　　节点对zk的心跳频率。默认2s一次

　　10、int tryCount

　　集群节点之间通信失败重试次数。默认2次

　　11、int clearScheduleBakTime

　　清理任务备份表中失效的leader备份。默认1小时一次。单位毫秒

　　12、ExecutorService clusterPool

　　集群公用程池。暂时不用设置

　　13、ExecutorService dispatchs

　　环形队列任务调度线程池。默认为cpu核心数

　　14、ExecutorService workers

　　环形队列工作任务线程池。默认为cpu核心数2倍
###任务超类(Task)
　　1、void setEndTimestamp(long endTimestamp)

　　设置任务未来的执行时间戳。如果你设置的是一个过去的时间戳，则代表任务立即执行。适用于单次任务以及周期性任务

　　2、void setTaskType(TaskType taskType)

　　设置任务类型。单次任务或周期任务

　　3、void setUnit(TimeUnit unit)

　　设置周期任务的执行周期时间单位。支持天、小时、分、秒

　　4、void setParam(Map<String,String> param)

　　设置当前任务携带的执行参数。仅支持字符串类型。其他类型都可以转化为字符串
###环形队列监控(AnnularQueueMonitor)
　　1、int getTaskInAnnularQueueQty()

　　获取环形队列中等待被触发执行的任务数

　　2、Map<String, String> getDispatchsPoolInfo()

　　获取分派任务线程池信息。包括：taskQty队列中等待执行的任务数，completedQty已经执行完成的任务数，activeQty正在执行任务的线程数，coreSize设置的核心线程数

　　3、Map<String, String> getWorkersPoolInfo()

　　获取执行任务线程池信息。包括：taskQty队列中等待执行的任务数，completedQty已经执行完成的任务数，activeQty正在执行任务的线程数，coreSize设置的核心线程数

###集群监控（ClusterMonitor）
　　1、String getCurrentNodeInfo()

　　获取当前节点信息

　　2、Map<String, Map<String, List>> getDBTraceInfoByTaskId(String taskId)

　　获取任务的数据存储跟踪信息。因为涉及到访问数据库，不建议频繁调用

　　3、ZKNode getCurrentZKNodeInfo()

　　获取当前节点在zk上的注册信息

###数据存储监控(DBMonitor)
　　1、Map<String,List> getInfoByTaskId(String taskId)

　　获取任务在本节点的数据存储信息
## 注意(Notice)

* 此构件已在Windows和centos下做了适当测试，如需使用，请自行测试
* 更多详细的介绍资料请访问网址:https://www.cnblogs.com/liuche/
* 如需联系作者请加QQ：827756467
