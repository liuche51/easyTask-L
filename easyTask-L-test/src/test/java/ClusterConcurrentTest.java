import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.monitor.ClusterMonitor;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.core.TaskType;
import com.github.liuche51.easyTask.core.TimeUnit;
import com.github.liuche51.easyTask.test.task.CusTask1;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 集群测试。模拟三个节点的伪集群
 */
public class ClusterConcurrentTest {
    private static Logger log = LoggerFactory.getLogger(ClusterConcurrentTest.class);

    @Test
    public void startNode1() {
        AnnularQueue annularQueue = AnnularQueue.getInstance();
        EasyTaskConfig config =new EasyTaskConfig();
        try {
            config.setTaskStorePath("C:/db/node1");
            config.setServerPort(2021);
            initData(annularQueue,config,"Node1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startNode2() {
        AnnularQueue annularQueue = AnnularQueue.getInstance();
        EasyTaskConfig config = new EasyTaskConfig();
        try {
            config.setTaskStorePath("C:/db/node2");
            config.setServerPort(2022);
            initData(annularQueue,config,"Node2");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startNode3() {
        AnnularQueue annularQueue = AnnularQueue.getInstance();
        EasyTaskConfig config = new EasyTaskConfig();
        try {
            config.setTaskStorePath("C:/db/node3");
            config.setServerPort(2023);
            initData(annularQueue,config,"Node3");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startNode4() {
        AnnularQueue annularQueue = AnnularQueue.getInstance();
        EasyTaskConfig config =new EasyTaskConfig();
        try {
            config.setTaskStorePath("C:/db/node4");
            config.setServerPort(2024);
            initData(annularQueue,config,"Node4");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initData(AnnularQueue annularQueue,EasyTaskConfig config,String name) throws Exception {
        config.setSQLlitePoolSize(5);
        config.setBackupCount(2);
        config.setZkAddress("127.0.0.1:2181");
        config.setDispatchs(new ThreadPoolExecutor(4, 4, 1000, java.util.concurrent.TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()));
        config.setWorkers(new ThreadPoolExecutor(4, 8, 1000, java.util.concurrent.TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()));
        annularQueue.start(config);
        //触发一次
        startOnceTask(5, 2,10, name);
        startPeriodTask(5,2,false,10,  name);
        //JUnit默认是非守护线程启动和Main方法不同。这里防止当前主线程退出导致子线程也退出了
        while (true) {
            Thread.sleep(1000);
            try {
                //触发多次
                //startOnceTask(5, 2,10, name);
               // startPeriodTask(5,2,false,10,  name);
            } catch (Exception e) {
                e.printStackTrace();
            }
            printinfo();
        }
    }
    private void startOnceTask(int threadqty,int count,int time,String name){
        List<Thread> list = new ArrayList<>(threadqty);
        Map<String, String> param = new HashMap<String, String>() {
            {
                put("name", name);
                put("birthday", "1996-1-1");
                put("age", "28");
                put("threadid", String.valueOf(Thread.currentThread().getId()));
            }
        };
        for (int i = 0; i < threadqty; i++) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int y=0;y<count;y++){
                        CusTask1 task1 = new CusTask1();
                        task1.setEndTimestamp(ZonedDateTime.now().plusSeconds(time).toInstant().toEpochMilli());
                        task1.setParam(param);
                        try {
                            AnnularQueue.getInstance().submitAllowWait(task1);
                        } catch (Exception e) {
                            log.error("", e);
                        }
                    }
                }
            });
            list.add(th);
        }
        list.forEach(x -> {
            x.start();
        });
    }
    private void startPeriodTask(int threadqty,int count,boolean immediately,int period, String name){
        List<Thread> list = new ArrayList<>(threadqty);
        Map<String, String> param2 = new HashMap<String, String>() {
            {
                put("name", name);
                put("birthday", "1996-1-1");
                put("age", "28");
                put("threadid", String.valueOf(Thread.currentThread().getId()));
            }
        };

        for (int i = 0; i < threadqty; i++) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int y=0;y<count;y++){
                        CusTask1 task2 = new CusTask1();
                        task2.setEndTimestamp(ZonedDateTime.now().plusSeconds(immediately? -1 : 1).toInstant().toEpochMilli());
                        task2.setTaskType(TaskType.PERIOD);
                        task2.setUnit(TimeUnit.SECONDS);
                        task2.setParam(param2);
                        try {
                            task2.setPeriod(period);
                            AnnularQueue.getInstance().submitAllowWait(task2);
                        } catch (Exception e) {
                            log.error("", e);
                        }
                    }
                }
            });
            list.add(th);
        }
        list.forEach(x -> {
            x.start();
        });
    }

    private void printinfo() {
        //log.info("集群节点信息：" + ClusterMonitor.getCurrentNodeInfo());
        log.info("Netty客户端连接池信息："+JSONObject.toJSONString(ClusterMonitor.getNettyClientPoolInfo()));
    }
}
