package com.github.liuche51.easyTask.dto;

public class Schedule {
    private String id;
    private String classPath;
    private long executeTime;
    private String taskType;
    private long period;
    private String unit;
    private String param;
    private String backup;
    private String source;
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

    public String getBackup() {
        return backup;
    }

    public void setBackup(String backup) {
        this.backup = backup;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
    public static Schedule valueOf(Task task){
        Schedule schedule=new Schedule();
        schedule.id=task.getScheduleExt().getId();
        schedule.classPath=task.getScheduleExt().getTaskClassPath();
        schedule.executeTime=task.getEndTimestamp();
        schedule.taskType=task.getTaskType().name();
        schedule.period=task.getPeriod();
        schedule.unit=task.getUnit() == null ? "" : task.getUnit().name();
        schedule.param=Task.serializeMap(task.getParam());
        schedule.backup=task.getScheduleExt().getBackup();
        schedule.source=task.getScheduleExt().getSource();
        return schedule;
    }

    /**
     * 转换为protocol buffer对象
     * @return
     */
    public ScheduleDto.Schedule toScheduleDto(){
        ScheduleDto.Schedule.Builder builder=ScheduleDto.Schedule.newBuilder();
        builder.setId(this.id).setClassPath(this.classPath).setExecuteTime(this.executeTime)
                .setTaskType(this.taskType).setPeriod(this.period).setUnit(this.unit)
                .setParam(this.param).setBackup(this.backup).setSource(this.source);
        return builder.build();
    }
}
