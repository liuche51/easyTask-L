package com.github.liuche51.easyTask.test;

import com.github.liuche51.easyTask.core.*;
import com.github.liuche51.easyTask.test.task.CusTask1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class Main {
    private static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            Properties properties = new Properties();
            // 使用ClassLoader加载properties配置文件生成对应的输入流
            InputStream in = Main.class.getClassLoader().getResourceAsStream("resources/sys.properties");
            // 使用properties对象加载输入流
            properties.load(in);
            AnnularQueue annularQueue = AnnularQueue.getInstance();
            EasyTaskConfig config = AnnularQueue.getInstance().getConfig();
            String path = properties.getProperty("taskStorePath");
            String serverPort = properties.getProperty("serverPort");
            String name = properties.getProperty("name");
            config.setTaskStorePath(path);
            config.setServerPort(Integer.valueOf(serverPort));
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
            task1.setEndTimestamp(ZonedDateTime.now().plusSeconds(10).toInstant().toEpochMilli());//10秒后执行
            Map<String, String> param = new HashMap<String, String>() {
                {
                    put("name", name);
                    put("birthday", "1988-1-1");
                    put("age", "25");
                    put("threadid", String.valueOf(Thread.currentThread().getId()));
                }
            };
            task1.setParam(param);
            CusTask1 task2 = new CusTask1();
            task2.setPeriod(30);
            task2.setEndTimestamp(ZonedDateTime.now().plusSeconds(10).toInstant().toEpochMilli());
            task2.setTaskType(TaskType.PERIOD);
            task2.setUnit(TimeUnit.SECONDS);
            Map<String, String> param2 = new HashMap<String, String>() {
                {
                    put("name", name);
                    put("birthday", "1986-1-1");
                    put("age", "32");
                    put("threadid", String.valueOf(Thread.currentThread().getId()));
                }
            };
            task2.setParam(param2);
      /*  try {
            annularQueue.submitAllowWait(task2);//单次提交测试
        } catch (Exception e) {
            e.printStackTrace();
        }*/
            //JUnit默认是非守护线程启动和Main方法不同。这里防止当前主线程退出导致子线程也退出了
            while (true) {
                Thread.sleep(1000);
                try {
                    annularQueue.submitAllowWait(task1);//多次提交测试
                    annularQueue.submitAllowWait(task2);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
