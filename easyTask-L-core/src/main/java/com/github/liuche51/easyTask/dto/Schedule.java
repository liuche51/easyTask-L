package com.github.liuche51.easyTask.dto;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;

import java.net.UnknownHostException;

public class Schedule {
    private String id;
    private String classPath;
    private long executeTime;
    private String taskType;
    /**
     *任务类型。0一次性任务，1周期性任务
     */
    private long period;
    /**
     * 周期任务时间单位。TimeUnit枚举
     */
    private String unit;
    private String param;
    private String transactionId;
    private String createTime;
    private String modifyTime;
    private String source;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClassPath() {
        return classPath;
    }

    public void setClassPath(String classPath) {
        this.classPath = classPath;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getParam() {
        return param;
    }

    public void setParam(String param) {
        this.param = param;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(String modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public static Schedule valueOf(Task task){
        Schedule schedule=new Schedule();
        schedule.id=task.getTaskExt().getId();
        schedule.classPath=task.getTaskExt().getTaskClassPath();
        schedule.executeTime=task.getEndTimestamp();
        schedule.taskType=task.getTaskType().name();
        schedule.period=task.getPeriod();
        schedule.unit=task.getUnit() == null ? "" : task.getUnit().name();
        schedule.param= JSONObject.toJSONString(task.getParam());
        schedule.setSource(task.getTaskExt().getSource());
        return schedule;
    }

    /**
     * 转换为protocol buffer对象
     * @return
     */
    public ScheduleDto.Schedule toScheduleDto() throws UnknownHostException {
        ScheduleDto.Schedule.Builder builder=ScheduleDto.Schedule.newBuilder();
        builder.setId(this.id).setClassPath(this.classPath).setExecuteTime(this.executeTime)
                .setTaskType(this.taskType).setPeriod(this.period).setUnit(this.unit)
                .setParam(this.param).setSource(AnnularQueue.getInstance().getConfig().getAddress())
                .setTransactionId(this.transactionId);
        return builder.build();
    }
}
