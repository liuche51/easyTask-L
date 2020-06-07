package com.github.liuche51.easyTask.core;


import com.github.liuche51.easyTask.backup.server.NettyServer;
import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.dao.DbInit;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.register.ZKService;
import com.github.liuche51.easyTask.register.ZKUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.*;

/**
 * 环形任务队列
 */
public class AnnularQueue {
    private static Logger log = LoggerFactory.getLogger(AnnularQueue.class);
    private static AnnularQueue singleton = null;
    public static boolean isRunning = false;//防止所线程运行环形队列
    /**
     * 任务调度线程池
     */
    private static ExecutorService dispatchs = null;
    /**
     * 工作任务线程池
     */
    private static ExecutorService workers = null;
    static Slice[] slices = new Slice[60];

    static {
        for (int i = 0; i < slices.length; i++) {
            slices[i] = new Slice();
        }
    }

    public static AnnularQueue getInstance() {
        if (singleton == null) {
            synchronized (AnnularQueue.class) {
                if (singleton == null) {
                    singleton = new AnnularQueue();
                }
            }
        }
        return singleton;
    }

    private AnnularQueue() {
    }

    Slice[] getSlices() {
        return slices;
    }

    ExecutorService getDispatchs() {
        return dispatchs;
    }

    ExecutorService getWorkers() {
        return workers;
    }

    /**
     * set the Dispatch ThreadPool
     *
     * @param dispatchs
     */
    public void setDispatchThreadPool(ThreadPoolExecutor dispatchs) throws Exception {
        if (isRunning)
            throw new Exception("please before AnnularQueue started set");
        this.dispatchs = dispatchs;
    }

    /**
     * set the Worker ThreadPool
     *
     * @param workers
     */
    public void setWorkerThreadPool(ThreadPoolExecutor workers) throws Exception {
        if (isRunning)
            throw new Exception("please before AnnularQueue started set");
        this.workers = workers;
    }

    private void setDefaultThreadPool() {
        if (this.dispatchs == null)
            this.dispatchs = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        if (this.workers == null)
            this.workers = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    }

    public void start(){
        Thread th1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    runQueue();
                } catch (Exception e) {
                    isRunning = false;
                    log.error("AnnularQueue start fail.", e);
                }
            }
        });
        th1.start();
    }

    /**
     * start the AnnularQueue
     */
    private synchronized void runQueue() throws Exception {
        //避免重复执行
        if (isRunning)
            return;
        NettyServer.getInstance().run();//启动组件的Netty服务端口
        ClusterService.initCurrentNode();
        DbInit.init();
        recover();
        setDefaultThreadPool();
        isRunning = true;
        int lastSecond = 0;
        while (true) {
            int second = ZonedDateTime.now().getSecond();
            if (second == lastSecond) {
                Thread.sleep(500l);
                continue;
            }
            Slice slice = slices[second];
            log.debug("已执行时间分片:{}，任务数量:{}", second, slice.getList() == null ? 0 : slice.getList().size());
            lastSecond = second;
            dispatchs.submit(new Runnable() {
                public void run() {
                    ConcurrentSkipListMap<String, Task> schedules = slice.getList();
                    List<Task> willremove = new LinkedList<>();
                    for (Map.Entry<String, Task> entry : schedules.entrySet()) {
                        Task s = entry.getValue();
                        if (System.currentTimeMillis() >= s.getEndTimestamp()) {
                            Runnable proxy = (Runnable) new ProxyFactory(s).getProxyInstance();
                            workers.submit(proxy);
                            willremove.add(s);
                            schedules.remove(entry.getKey());
                            log.debug("工作任务:{} 已提交分片:{}", s.getScheduleExt().getId(), second);
                        }
                        //因为列表是已经按截止执行时间排好序的，可以节省后面元素的过期判断
                        else break;
                    }
                    submitNewPeriodSchedule(willremove);
                }
            });
        }
    }

    /**
     * 客户端提交任务。允许线程等待，直到easyTask组件启动完成
     *
     * @param schedule
     * @return
     * @throws Exception
     */
    public String submitAllowWait(Task schedule) throws Exception {
        while (true) {
            if (!isRunning) Thread.sleep(1000);//如果未启动则休眠1s
            else return submit(schedule);
        }

    }

    /**
     * 客户端提交任务。如果easyTask组件未启动，则抛出异常
     *
     * @param task
     * @return
     * @throws Exception
     */
    public String submit(Task task) throws Exception {
        if (!isRunning) throw new Exception("the easyTask has not started,please wait a moment!");
        task.getScheduleExt().setId(Util.generateUniqueId());
        if (task.getTaskType().equals(TaskType.PERIOD)) {
            if (task.isImmediateExecute())
                task.setEndTimestamp(ZonedDateTime.now().toInstant().toEpochMilli());
            else
                task.setEndTimestamp(Task.getTimeStampByTimeUnit(task.getPeriod(), task.getUnit()));
        }
        String path = task.getClass().getName();
        task.getScheduleExt().setTaskClassPath(path);
        //以下两行代码不要调换，否则可能发生任务已经执行完成，而任务尚未持久化，导致无法执行删除持久化的任务风险
        ClusterService.save(task);
        AddSchedule(task);
        ZonedDateTime time = ZonedDateTime.ofInstant(new Timestamp(task.getEndTimestamp()).toInstant(), ZoneId.systemDefault());
        log.debug("已添加类型:{}任务:{}，所属分片:{} 预计执行时间:{} 线程ID:{}", task.getTaskType().name(), task.getScheduleExt().getId(), time.getSecond(), time.toLocalTime(), Thread.currentThread().getId());
        return task.getScheduleExt().getId();
    }

    /**
     * 批量创建新周期任务
     *
     * @param list
     */
    public void submitNewPeriodSchedule(List<Task> list) {
        for (Task schedule : list) {
            if (!TaskType.PERIOD.equals(schedule.getTaskType()))//周期任务需要重新提交新任务
                continue;
            try {
                schedule.setEndTimestamp(Task.getTimeStampByTimeUnit(schedule.getPeriod(), schedule.getUnit()));
                AddSchedule(schedule);
                int slice = AddSchedule(schedule);
                log.debug("已重新提交周期任务:{}，所属分片:{}，线程ID:{}", schedule.getScheduleExt().getId(), slice, Thread.currentThread().getId());
            } catch (Exception e) {
                log.error("submitNewPeriodSchedule exception！", e);
            }
        }
    }

    /**
     * 恢复中断后的系统任务
     */
    private void recover() {
        List<Schedule> list = ScheduleDao.selectAll();
        try {
            for (Schedule schedule : list) {
                try {
                    Task task = Task.valueOf(schedule);
                    Class c = Class.forName(task.getScheduleExt().getTaskClassPath());
                    Object o = c.newInstance();
                    Task schedule1 = (Task) o;//强转后设置id，o对象值也会变，所以强转后的task也是对象的引用而已
                    schedule1.getScheduleExt().setId(task.getScheduleExt().getId());
                    schedule1.setEndTimestamp(task.getEndTimestamp());
                    schedule1.setPeriod(task.getPeriod());
                    schedule1.setTaskType(task.getTaskType());
                    schedule1.setUnit(task.getUnit());
                    schedule1.getScheduleExt().setTaskClassPath(task.getScheduleExt().getTaskClassPath());
                    schedule1.setParam(task.getParam());
                    AddSchedule(schedule1);
                } catch (Exception e) {
                    log.error("schedule:{} recover fail.", schedule.getId());
                }
            }
            log.debug("easyTask recover success! count:{}", list.size());
        } catch (Exception e) {
            log.error("easyTask recover fail.");
        }
    }

    /**
     * 将任务添加到时间分片中去。
     *
     * @param schedule
     * @return
     */
    private int AddSchedule(Task schedule) {
        ZonedDateTime time = ZonedDateTime.ofInstant(new Timestamp(schedule.getEndTimestamp()).toInstant(), ZoneId.systemDefault());
        int second = time.getSecond();
        Slice slice = slices[second];
        ConcurrentSkipListMap<String, Task> list2 = slice.getList();
        list2.put(schedule.getEndTimestamp() + "-" + Util.GREACE.getAndIncrement(), schedule);
        return second;
    }
}
