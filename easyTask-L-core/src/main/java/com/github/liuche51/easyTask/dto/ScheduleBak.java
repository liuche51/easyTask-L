package com.github.liuche51.easyTask.dto;

import com.github.liuche51.easyTask.dto.proto.ScheduleDto;

public class ScheduleBak {
    private String id;
    private String classPath;
    private long executeTime;
    private String taskType;
    private long period;
    private String unit;
    private String param;
    private String createTime;

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

    public String getCreateTime() {
        return createTime;
    }
    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
    public static ScheduleBak valueOf(ScheduleDto.Schedule dto){
        ScheduleBak schedule=new ScheduleBak();
        schedule.id=dto.getId();
        schedule.classPath=dto.getClassPath();
        schedule.executeTime=dto.getExecuteTime();
        schedule.taskType=dto.getTaskType();
        schedule.period=dto.getPeriod();
        schedule.unit=dto.getUnit();
        schedule.param=dto.getParam();
        return schedule;
    }
}
