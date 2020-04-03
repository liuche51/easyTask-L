package com.github.liuche51.easyTask.register;

/**
 * ZK服务当前节点数据对象
 */
public class NodeData {
    private long backupCount;
    private long taskCount;

    public long getBackupCount() {
        return backupCount;
    }

    public void setBackupCount(long backupCount) {
        this.backupCount = backupCount;
    }

    public long getTaskCount() {
        return taskCount;
    }

    public void setTaskCount(long taskCount) {
        this.taskCount = taskCount;
    }
    @Override
    public String toString(){
         StringBuilder builder=new StringBuilder();
         builder.append(backupCount).append(",").append(taskCount);
         return builder.toString();
    }
}
