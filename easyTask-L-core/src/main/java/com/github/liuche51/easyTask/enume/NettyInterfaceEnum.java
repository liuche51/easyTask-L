package com.github.liuche51.easyTask.enume;

public class NettyInterfaceEnum {
    /**
     * 预备提交任务接口。阶段一
     */
    public static final String TRAN_TRY_PRESAVETASK="Tran_Try_PreSaveTask";
    /**
     * 同步任务数据备份接口
     */
    public static final String SYNC_SCHEDULE_BACKUP="SyncScheduleBackup";
    /**
     * 同步任务数据备份接口，批量方式
     */
    public static final String SYNC_SCHEDULE_BACKUP_BATCH="SyncScheduleBackupBatch";
    /**
     * 删除备份任务数据接口
     */
    public static final String DELETE_SCHEDULEBACKUP="DeleteScheduleBackup";
    /**
     * 同步leader位置信息接口
     */
    public static final String SYNC_LEADER_POSITION="SyncLeaderPosition";
}
