package com.github.liuche51.easyTask.core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * get some useful performance data
 */
public class Monitor {
    /**
     * @return
     */
    public static int getScheduleInAnnularQueueCount() {
        Slice[] slices = AnnularQueue.getInstance().getSlices();
        int count = 0;
        for (Slice slice : slices) {
            count += slice.getList().size();
        }
        return count;
    }

    /**
     * get all has persistent Schedule count
     *
     * @return
     */
    public static int getDBScheduleCount() {
        try {
            return ScheduleDao.getAllCount();
        } catch (Exception e) {
            return 0;
        }

    }

    /**
     * @return
     */
    public static int getDispatchsPoolWaiteToExecuteScheduleCount() {
        ExecutorService dispatchs = AnnularQueue.getInstance().getDispatchs();
        if (dispatchs == null)
            return 0;
        return ((ThreadPoolExecutor) dispatchs).getQueue().size();
    }

    /**
     * @return
     */
    public static int getWorkersPoolWaiteToExecuteScheduleCount() {
        ExecutorService worker = AnnularQueue.getInstance().getWorkers();
        if (worker == null)
            return 0;
        return ((ThreadPoolExecutor) worker).getQueue().size();
    }
    /**
     * @return
     */
    public static long getDispatchsPoolExecutedScheduleCount() {
        ExecutorService dispatchs = AnnularQueue.getInstance().getWorkers();
        if (dispatchs == null)
            return 0;
        return ((ThreadPoolExecutor) dispatchs).getCompletedTaskCount();
    }
    /**
     * @return
     */
    public static long getWorkersPoolExecutedScheduleCount() {
        ExecutorService worker = AnnularQueue.getInstance().getWorkers();
        if (worker == null)
            return 0;
        return ((ThreadPoolExecutor) worker).getCompletedTaskCount();
    }
}
