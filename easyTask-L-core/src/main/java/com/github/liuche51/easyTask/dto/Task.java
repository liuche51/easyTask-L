package com.github.liuche51.easyTask.dto;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.core.TaskType;
import com.github.liuche51.easyTask.core.TimeUnit;
import com.github.liuche51.easyTask.dao.ScheduleDao;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

public class Task {
    /**
     * 任务截止运行时间
     */
    private long endTimestamp;
    private TaskType taskType=TaskType.ONECE;
    /**
     * 是否立即执行一次
     */
    private boolean immediateExecute=false;
    private long period;
    private TimeUnit unit;
    private Runnable run;
   private TaskExt scheduleExt=new TaskExt();
    private Map<String,String> param;



    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }
    public TaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }
    public long getPeriod() {
        return period;
    }
    public boolean isImmediateExecute() {
        return immediateExecute;
    }

    public void setImmediateExecute(boolean immediateExecute) {
        this.immediateExecute = immediateExecute;
    }
    public void setPeriod(long period) {
        this.period = period;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public void setUnit(TimeUnit unit) {
        this.unit = unit;
    }
    public Map<String,String> getParam() {
        return param;
    }

    public TaskExt getScheduleExt() {
        return scheduleExt;
    }

    public void setScheduleExt(TaskExt scheduleExt) {
        this.scheduleExt = scheduleExt;
    }

    public void setParam(Map<String,String> param) {
        this.param = param;
    }
    /**
     * 获取周期性任务下次执行时间。已当前时间为基准计算下次而不是上次截止执行时间
     * @param period
     * @param unit
     * @return
     * @throws Exception
     */
    public static long getTimeStampByTimeUnit(long period,TimeUnit unit) throws Exception {
        if(period==0)
            throw new Exception("period can not zero");
        switch (unit)
        {
            case DAYS:
                return ZonedDateTime.now().plusDays(period).toInstant().toEpochMilli();
            case HOURS:
                return ZonedDateTime.now().plusHours(period).toInstant().toEpochMilli();
            case MINUTES:
                return ZonedDateTime.now().plusMinutes(period).toInstant().toEpochMilli();
            case SECONDS:
                return ZonedDateTime.now().plusSeconds(period).toInstant().toEpochMilli();
                default:throw new Exception("unSupport TimeUnit type");
        }
    }
    public static Task valueOf(Schedule schedule){
        Task task = new Task();
        task.getScheduleExt().setId(schedule.getId());
        task.getScheduleExt().setTaskClassPath(schedule.getClassPath());
        task.setEndTimestamp(schedule.getExecuteTime());
        task.setParam(JSONObject.parseObject(schedule.getParam(),Map.class));
        if ("PERIOD".equals(schedule.getTaskType()))
            task.setTaskType(TaskType.PERIOD);
        else if ("ONECE".equals(schedule.getTaskType()))
            task.setTaskType(TaskType.ONECE);
        task.setPeriod(schedule.getPeriod());
        task.setUnit(getTimeUnit(schedule.getUnit()));
        return task;
    }
    public static Task valueOf(ScheduleBak scheduleBak){
        Task task = new Task();
        task.getScheduleExt().setId(scheduleBak.getId());
        task.getScheduleExt().setTaskClassPath(scheduleBak.getClassPath());
        task.getScheduleExt().setSource(scheduleBak.getSource());
        task.setEndTimestamp(scheduleBak.getExecuteTime());
        task.setParam(JSONObject.parseObject(scheduleBak.getParam(),Map.class));
        if ("PERIOD".equals(scheduleBak.getTaskType()))
            task.setTaskType(TaskType.PERIOD);
        else if ("ONECE".equals(scheduleBak.getTaskType()))
            task.setTaskType(TaskType.ONECE);
        task.setPeriod(scheduleBak.getPeriod());
        task.setUnit(getTimeUnit(scheduleBak.getUnit()));
        return task;
    }

    private static TimeUnit getTimeUnit(String unit) {
        switch (unit) {
            case "DAYS":
               return TimeUnit.DAYS;
            case "HOURS":
                return TimeUnit.HOURS;
            case "MINUTES":
                return TimeUnit.MINUTES;
            case "SECONDS":
                return TimeUnit.SECONDS;
            default:return null;
        }
    }
}
