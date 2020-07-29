package com.github.liuche51.easyTask.dto;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.core.TaskType;
import com.github.liuche51.easyTask.core.TimeUnit;

import java.time.ZonedDateTime;
import java.util.Map;

public class Task {
    /**
     * 任务截止运行时间
     */
    private long endTimestamp;
    private TaskType taskType = TaskType.ONECE;
    private long period;
    private TimeUnit unit;
    private TaskExt taskExt = new TaskExt();
    private Map<String, String> param;


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

    public void setPeriod(long period) throws Exception {
        if (period <= 0)
            throw new Exception("period cannot less than 0！");
        this.period = period;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public void setUnit(TimeUnit unit) {
        this.unit = unit;
    }

    public Map<String, String> getParam() {
        return param;
    }

    public TaskExt getTaskExt() {
        return taskExt;
    }

    public void setParam(Map<String, String> param) {
        this.param = param;
    }

    /**
     * 获取周期性任务下次执行时间。已当前时间为基准计算下次而不是上次截止执行时间
     *
     * @param period
     * @param unit
     * @return
     * @throws Exception
     */
    public static long getNextExcuteTimeStamp(long period, TimeUnit unit) throws Exception {
        switch (unit) {
            case DAYS:
                return ZonedDateTime.now().plusDays(period).toInstant().toEpochMilli();
            case HOURS:
                return ZonedDateTime.now().plusHours(period).toInstant().toEpochMilli();
            case MINUTES:
                return ZonedDateTime.now().plusMinutes(period).toInstant().toEpochMilli();
            case SECONDS:
                return ZonedDateTime.now().plusSeconds(period).toInstant().toEpochMilli();
            default:
                throw new Exception("unSupport TimeUnit type");
        }
    }

    public static Task valueOf(ScheduleBak scheduleBak) throws Exception {
        Task task = new Task();
        task.getTaskExt().setId(scheduleBak.getId());
        task.getTaskExt().setTaskClassPath(scheduleBak.getClassPath());
        task.getTaskExt().setSource(scheduleBak.getSource());
        task.setEndTimestamp(scheduleBak.getExecuteTime());
        task.setParam(JSONObject.parseObject(scheduleBak.getParam(), Map.class));
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
            default:
                return null;
        }
    }
}
