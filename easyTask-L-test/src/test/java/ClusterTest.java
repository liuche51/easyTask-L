import com.github.liuche51.easyTask.cluster.ClusterMonitor;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.core.TaskType;
import com.github.liuche51.easyTask.core.TimeUnit;
import com.github.liuche51.easyTask.test.task.CusTask1;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 集群测试。模拟三个节点的伪集群
 */
public class ClusterTest {
    private static Logger log = LoggerFactory.getLogger(ClusterTest.class);

    @Test
    public void startNode1() {
        AnnularQueue annularQueue = AnnularQueue.getInstance();
        EasyTaskConfig config = EasyTaskConfig.getInstance();
        try {
            config.setTaskStorePath("C:\\db\\node1");
            config.setServerPort(2021);
            initData(annularQueue);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startNode2() {
        AnnularQueue annularQueue = AnnularQueue.getInstance();
        EasyTaskConfig config = EasyTaskConfig.getInstance();
        try {
            config.setTaskStorePath("C:\\db\\node2");
            config.setServerPort(2022);
            initData(annularQueue);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startNode3() {
        AnnularQueue annularQueue = AnnularQueue.getInstance();
        EasyTaskConfig config = EasyTaskConfig.getInstance();
        try {
            config.setTaskStorePath("C:\\db\\node3");
            config.setServerPort(2023);
            initData(annularQueue);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void startNode4() {
        AnnularQueue annularQueue = AnnularQueue.getInstance();
        EasyTaskConfig config = EasyTaskConfig.getInstance();
        try {
            config.setTaskStorePath("C:\\db\\node4");
            config.setServerPort(2024);
            initData(annularQueue);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initData(AnnularQueue annularQueue) throws Exception {
        EasyTaskConfig config = EasyTaskConfig.getInstance();
        config.setSQLlitePoolSize(5);
        config.setBackupCount(2);
        //config.setDeleteZKTimeOunt(500);
        //config.setSelectLeaderZKNodeTimeOunt(500);
        annularQueue.setDispatchThreadPool(new ThreadPoolExecutor(4, 4, 1000, java.util.concurrent.TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()));
        annularQueue.setWorkerThreadPool(new ThreadPoolExecutor(4, 8, 1000, java.util.concurrent.TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()));
        annularQueue.start();
        CusTask1 task1 = new CusTask1();
        task1.setEndTimestamp(ZonedDateTime.now().plusSeconds(10).toInstant().toEpochMilli());
        Map<String, String> param = new HashMap<String, String>() {
            {
                put("name", "刘彻");
                put("birthday", "1988-1-1");
                put("age", "25");
                put("threadid", String.valueOf(Thread.currentThread().getId()));
            }
        };
        task1.setParam(param);
        CusTask1 task2 = new CusTask1();
        task2.setPeriod(30);
        task2.setImmediateExecute(true);
        task2.setTaskType(TaskType.PERIOD);
        task2.setUnit(TimeUnit.SECONDS);
        Map<String, String> param2 = new HashMap<String, String>() {
            {
                put("name", "Jack");
                put("birthday", "1986-1-1");
                put("age", "32");
                put("threadid", String.valueOf(Thread.currentThread().getId()));
            }
        };
        task2.setParam(param2);
        try {
           // annularQueue.submitAllowWait(task1);//单次提交测试
        } catch (Exception e) {
            e.printStackTrace();
        }

        //annularQueue.submitAllowWait(task2);
        //JUnit默认是非守护线程启动和Main方法不同。这里防止当前主线程退出导致子线程也退出了
        while (true) {
            Thread.sleep(5000);
            try {
                //annularQueue.submitAllowWait(task1);//多次提交测试
            } catch (Exception e) {
                e.printStackTrace();
            }
            printinfo();
        }
    }

    private void printinfo() {
        log.info("集群节点信息：" + ClusterMonitor.getCurrentNodeInfo());
    }
}
