package com.github.liuche51.easyTask.core;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
    private static boolean isRunning = false;//防止所线程运行环形队列
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
    Slice[] getSlices(){
        return slices;
    }
    ExecutorService getDispatchs(){
        return dispatchs;
    }
    ExecutorService getWorkers(){
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

    /**
     * set Task Store Path.example  C:\\db
     * @param path
     * @throws Exception
     */
    public void setTaskStorePath(String path) throws Exception {
        if (isRunning)
            throw new Exception("please before AnnularQueue started set");
        SQLlitePool.dbFilePath = path + "\\easyTask.db";
        File file = new File(path);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    /**
     * set SQLlitePool Size，default qty 15
     * @param count
     * @throws Exception
     */
    public void setSQLlitePoolSize(int count) throws Exception{
        if (isRunning)
            throw new Exception("please before AnnularQueue started set");
        if(count<1)
            throw new Exception("poolSize must >1");
        SQLlitePool.poolSize=count;
    }
    private void setDefaultThreadPool() {
        if (this.dispatchs == null)
            this.dispatchs = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        if (this.workers == null)
            this.workers = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    }

    public void start() {
        Thread th1 = new Thread(new Runnable() {
            @Override
            public void run() {
                runQueue();
            }
        });
        th1.start();
    }

    /**
     * start the AnnularQueue
     */
    private synchronized void runQueue() {
        //避免重复执行
        if (isRunning)
            return;
        try {
            DbInit.init();
            recover();
            isRunning = true;
            setDefaultThreadPool();
            int lastSecond = 0;
            while (true) {
                int second = ZonedDateTime.now().getSecond();
                if (second == lastSecond) {
                    try {
                        Thread.sleep(500l);
                        continue;
                    } catch (Exception e) {

                    }
                }
                Slice slice = slices[second];
                log.debug("已执行时间分片:{}，任务数量:{}", second, slice.getList() == null ? 0 : slice.getList().size());
                lastSecond = second;
                dispatchs.submit(new Runnable() {
                    public void run() {
                        ConcurrentSkipListMap<String,Schedule> schedules = slice.getList();
                        List<Schedule> willremove = new LinkedList<>();
                        for (Map.Entry<String,Schedule> entry:schedules.entrySet()) {
                            Schedule s=entry.getValue();
                            if (System.currentTimeMillis() >= s.getEndTimestamp()) {
                                Runnable proxy = (Runnable) new ProxyFactory(s).getProxyInstance();
                                workers.submit(proxy);
                                willremove.add(s);
                                schedules.remove(entry.getKey());
                                log.debug("工作任务:{} 已提交分片:{}", s.getScheduleExt().getId(),second);
                            }
                            //因为列表是已经按截止执行时间排好序的，可以节省后面元素的过期判断
                            else break;
                        }
                        submitNewPeriodSchedule(willremove);
                    }
                });
            }

        } catch (
                Exception e) {
            isRunning = false;
            log.error("AnnularQueue start fail.", e);
            throw e;
        }

    }

    public String submit(Schedule schedule) throws Exception {
        schedule.getScheduleExt().setId(Util.generateUniqueId());
        if (schedule.getTaskType().equals(TaskType.PERIOD)) {
            if (schedule.isImmediateExecute())
                schedule.setEndTimestamp(ZonedDateTime.now().toInstant().toEpochMilli());
            else
                schedule.setEndTimestamp(Schedule.getTimeStampByTimeUnit(schedule.getPeriod(), schedule.getUnit()));
        }
        String path = schedule.getClass().getName();
        schedule.getScheduleExt().setTaskClassPath(path);
        //以下两行代码不要调换，否则可能发生任务已经执行完成，而任务尚未持久化，导致无法执行删除持久化的任务风险
        schedule.save();
        AddSchedule(schedule);
        ZonedDateTime time = ZonedDateTime .ofInstant(new Timestamp(schedule.getEndTimestamp()).toInstant(), ZoneId.systemDefault());
        log.debug("已添加类型:{}任务:{}，所属分片:{} 预计执行时间:{} 线程ID:{}",schedule.getTaskType().name(), schedule.getScheduleExt().getId(), time.getSecond(), time.toLocalTime(),Thread.currentThread().getId());
        return schedule.getScheduleExt().getId();
    }

    /**
     * 批量创建新周期任务
     *
     * @param list
     */
    public void submitNewPeriodSchedule(List<Schedule> list) {
        for (Schedule schedule : list) {
            if (!TaskType.PERIOD.equals(schedule.getTaskType()))//周期任务需要重新提交新任务
                continue;
            try {
                schedule.setEndTimestamp(Schedule.getTimeStampByTimeUnit(schedule.getPeriod(), schedule.getUnit()));
                AddSchedule(schedule);
                int slice=AddSchedule(schedule);
                log.debug("已重新提交周期任务:{}，所属分片:{}，线程ID:{}", schedule.getScheduleExt().getId(),slice,Thread.currentThread().getId());
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
                    Class c = Class.forName(schedule.getScheduleExt().getTaskClassPath());
                    Object o = c.newInstance();
                    Schedule schedule1 = (Schedule) o;//强转后设置id，o对象值也会变，所以强转后的task也是对象的引用而已
                   schedule1.getScheduleExt().setId(schedule.getScheduleExt().getId());
                    schedule1.setEndTimestamp(schedule.getEndTimestamp());
                    schedule1.setPeriod(schedule.getPeriod());
                    schedule1.setTaskType(schedule.getTaskType());
                    schedule1.setUnit(schedule.getUnit());
                    schedule1.getScheduleExt().setTaskClassPath(schedule.getScheduleExt().getTaskClassPath());
                    schedule1.setParam(schedule.getParam());
                    AddSchedule(schedule1);
                } catch (Exception e) {
                    log.error("schedule:{} recover fail.", schedule.getScheduleExt().getId());
                }
            }
            log.debug("easyTask recover success! count:{}", list.size());
        } catch (Exception e) {
            log.error("easyTask recover fail.");
        }
    }

    /**
     * 将任务添加到时间分片中去。
     * @param schedule
     * @return
     */
    private int AddSchedule(Schedule schedule) {
        ZonedDateTime time =ZonedDateTime .ofInstant(new Timestamp(schedule.getEndTimestamp()).toInstant(), ZoneId.systemDefault());
        int second = time.getSecond();
        Slice slice = slices[second];
        ConcurrentSkipListMap<String,Schedule> list2 = slice.getList();
        list2.put(schedule.getEndTimestamp()+"-"+Util.GREACE.getAndIncrement(),schedule);
        return second;
    }
}
