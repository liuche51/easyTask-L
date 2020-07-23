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
* 高可用：因为我们是分布式leader-follow集群，每个任务多有多个备份数据，所以可靠性非常高
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
## 注意(Notice)

* 此构件已在Windows和centos下做了适当测试，如需使用，请自行测试