package com.github.liuche51.easyTask.monitor;

import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.Slice;
import com.github.liuche51.easyTask.dao.ScheduleDao;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * get some useful performance data
 */
public class AnnularQueueMonitor {
    /**
     * 获取环形队列中待执行的任务数
     * @return
     */
    public static int getTaskInAnnularQueueQty() {
        Slice[] slices = AnnularQueue.getInstance().getSlices();
        int count = 0;
        for (Slice slice : slices) {
            count += slice.getList().size();
        }
        return count;
    }

    /**
     * 获取任务分派线程池信息
     * @return
     */
    public static Map<String,String> getDispatchsPoolInfo() {
        Map<String,String> map=new HashMap<>();
        ExecutorService dispatchs = AnnularQueue.getInstance().getConfig().getDispatchs();
        if (dispatchs == null)
            return null;
        ThreadPoolExecutor tp=(ThreadPoolExecutor) dispatchs;
        map.put("taskQty",String.valueOf(tp.getQueue().size()));//队列中等待执行的任务数
        map.put("completedQty",String.valueOf(tp.getCompletedTaskCount()));//已经执行完成的任务数
        map.put("activeQty",String.valueOf(tp.getActiveCount()));//正在执行任务的线程数
        map.put("coreSize",String.valueOf(tp.getCorePoolSize()));//设置的核心线程数
        return map;
    }
    /**
     * 获取任务执行线程池信息
     * @return
     */
    public static Map<String,String> getWorkersPoolInfo() {
        Map<String,String> map=new HashMap<>();
        ExecutorService workers = AnnularQueue.getInstance().getConfig().getWorkers();
        if (workers == null)
            return null;
        ThreadPoolExecutor tp=(ThreadPoolExecutor) workers;
        map.put("taskQty",String.valueOf(tp.getQueue().size()));//队列中等待执行的任务数
        map.put("completedQty",String.valueOf(tp.getCompletedTaskCount()));//已经执行完成的任务数
        map.put("activeQty",String.valueOf(tp.getActiveCount()));//正在执行任务的线程数
        map.put("coreSize",String.valueOf(tp.getCorePoolSize()));//设置的核心线程数
        return map;
    }
}
