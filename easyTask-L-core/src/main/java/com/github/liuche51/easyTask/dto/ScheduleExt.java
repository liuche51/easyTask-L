package com.github.liuche51.easyTask.dto;

public class ScheduleExt {
    private String id;
    private String taskClassPath;
    private String backup;
    private String source;
    private volatile String oldId;
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getTaskClassPath() {
        return taskClassPath;
    }

    public void setTaskClassPath(String taskClassPath) {
        this.taskClassPath = taskClassPath;
    }  public String getOldId() {
        return oldId;
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

    public void setOldId(String oldId) {
        this.oldId = oldId;
    }
}
