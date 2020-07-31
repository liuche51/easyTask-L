import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dao.DbInit;
import com.github.liuche51.easyTask.dao.TransactionLogDao;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.dto.TransactionLog;
import com.github.liuche51.easyTask.enume.TransactionStatusEnum;
import com.github.liuche51.easyTask.monitor.ClusterMonitor;
import com.github.liuche51.easyTask.util.DateUtils;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class UnitTest {
    public static List<Thread> threadList = new LinkedList<>();

    @Test
    public void test() {
        try {
            EasyTaskConfig config = new EasyTaskConfig();
            config.setTaskStorePath("C:/db/node1");
            //AnnularQueue.getInstance().setConfig(config);
            ClusterMonitor.getDBTraceInfoByTaskId("229d631f1d264a8a922d0c4c5f752b45-19");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void test2() {
        try {
            ConcurrentSkipListMap<Long, Task> list = new ConcurrentSkipListMap<>();
            Thread th1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        list.put(System.currentTimeMillis(), new Task());
                        try {
                            Thread.sleep(10l);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            th1.start();
            Thread.sleep(10000l);
            Thread th2 = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            Thread.sleep(1000l);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        System.out.println("list.size()=" + list.size());
                       /* for (Map.Entry<Long, Task> entry : list.entrySet()) {
                            if (entry.getKey() % 2 == 0)
                            {
                                list.remove(entry.getKey());
                                System.out.println(" list.remove()="+entry.getKey());
                            }
                        }*/
                        Iterator<Map.Entry<Long, Task>> items = list.entrySet().iterator();
                        while (items.hasNext()) {
                            Map.Entry<Long, Task> item = items.next();
                            if (item.getKey() % 2 == 0) {
                                items.remove();
                                System.out.println(" list.remove()=" + item.getKey());
                            }
                        }
                    }
                }
            });
            th2.start();
            while (true) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    @Test
    public void test3() {
        try {
            Map<Long, Task> list = new ConcurrentHashMap<>();
            Thread th1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        list.put(System.currentTimeMillis(), new Task());
                        try {
                            Thread.sleep(10l);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            th1.start();
            Thread.sleep(5000l);
            Thread th2 = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            Thread.sleep(1000l);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        System.out.println("list.size()=" + list.size());
                        for (Map.Entry<Long, Task> entry : list.entrySet()) {
                            if (entry.getKey() % 2 == 0)
                            {
                                list.remove(entry.getKey());
                                System.out.println(" list.remove()="+entry.getKey());
                            }
                        }
                       /* Iterator<Map.Entry<Long, Task>> items = list.entrySet().iterator();
                        while (items.hasNext()) {
                            Map.Entry<Long, Task> item = items.next();
                            if (item.getKey() % 2 == 0) {
                                items.remove();
                                System.out.println(" list.remove()=" + item.getKey());
                            }
                        }*/
                    }
                }
            });
            th2.start();
            Thread th3 = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            Thread.sleep(1000l);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                       /* System.out.println("list.size()=" + list.size());
                        for (Map.Entry<Long, Task> entry : list.entrySet()) {
                            if (entry.getKey() % 2 == 0)
                            {
                                list.remove(entry.getKey());
                                System.out.println(" list.remove()="+entry.getKey());
                            }
                        }*/
                        Iterator<Map.Entry<Long, Task>> items = list.entrySet().iterator();
                        while (items.hasNext()) {
                            Map.Entry<Long, Task> item = items.next();
                            if (item.getKey() % 2 == 0) {
                                items.remove();
                                System.out.println(" list.remove()=" + item.getKey());
                            }
                        }
                    }
                }
            });
            th3.start();
            while (true) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

