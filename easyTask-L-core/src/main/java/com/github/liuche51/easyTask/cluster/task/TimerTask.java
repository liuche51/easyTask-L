package com.github.liuche51.easyTask.cluster.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public abstract class TimerTask extends Thread{
    protected static final Logger log = LoggerFactory.getLogger(HeartbeatsTask.class);
    //volatile修饰符用来保证其它线程读取的总是该变量的最新的值
    private volatile boolean exit = false;
    /**
     * 定时任务最近一次运行实际
     */
    private Date lastRunTime=new Date();

    protected boolean isExit() {
        return exit;
    }

    public void setExit(boolean exit) {
        this.exit = exit;
    }

    public Date getLastRunTime() {
        return lastRunTime;
    }

    public void setLastRunTime(Date lastRunTime) {
        this.lastRunTime = lastRunTime;
    }
}
