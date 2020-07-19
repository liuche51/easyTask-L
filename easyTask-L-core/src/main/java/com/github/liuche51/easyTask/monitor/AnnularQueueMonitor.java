package com.github.liuche51.easyTask.monitor;

import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.Slice;
import com.github.liuche51.easyTask.dao.ScheduleDao;

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
    public static int getTaskInAnnularQueueCount() {
        Slice[] slices = AnnularQueue.getInstance().getSlices();
        int count = 0;
        for (Slice slice : slices) {
            count += slice.getList().size();
        }
        return count;
    }

    /**
     * 获取分派线程池中待执行的任务数
     * @return
     */
    public static int getDispatchsPoolWaiteToExecuteTaskCount() {
        ExecutorService dispatchs = AnnularQueue.getInstance().getConfig().getDispatchs();
        if (dispatchs == null)
            return 0;
        return ((ThreadPoolExecutor) dispatchs).getQueue().size();
    }

    /**
     * 获取工作线程池中待执行的任务数
     * @return
     */
    public static int getWorkersPoolWaiteToExecuteTaskCount() {
        ExecutorService worker = AnnularQueue.getInstance().getConfig().getWorkers();
        if (worker == null)
            return 0;
        return ((ThreadPoolExecutor) worker).getQueue().size();
    }
    /**
     * @return
     */
    public static long getDispatchsPoolExecutedTaskCount() {
        ExecutorService dispatchs = AnnularQueue.getInstance().getConfig().getWorkers();
        if (dispatchs == null)
            return 0;
        return ((ThreadPoolExecutor) dispatchs).getCompletedTaskCount();
    }
    /**
     * @return
     */
    public static long getWorkersPoolExecutedTaskCount() {
        ExecutorService worker = AnnularQueue.getInstance().getConfig().getWorkers();
        if (worker == null)
            return 0;
        return ((ThreadPoolExecutor) worker).getCompletedTaskCount();
    }
}
