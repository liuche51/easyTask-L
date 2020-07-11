package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.core.AnnularQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbInit {
    private static Logger log = LoggerFactory.getLogger(AnnularQueue.class);
    public static boolean hasInit = false;//数据库是否已经初始化

    /**
     * 数据库初始化。需要避免多线程
     *
     * @return
     */
    public static synchronized boolean init() {
        if (hasInit)
            return true;
        try {
            SQLlitePool.getInstance().init();
            boolean exist = ScheduleDao.existTable();
            if (!exist) {
                //本地待运行的任务
                String sql = "CREATE TABLE \"schedule\" (\n" +
                        "\"id\"  TEXT NOT NULL,\n" +
                        "\"class_path\"  TEXT,\n" +
                        "\"execute_time\"  INTEGER,\n" +
                        "\"task_type\"  TEXT,\n" +
                        "\"period\"  INTEGER,\n" +
                        "\"unit\"  TEXT,\n" +
                        "\"param\"  TEXT,\n" +
                        "\"transaction_id\"  TEXT,\n" +
                        "\"create_time\"  TEXT,\n" +
                        "\"modify_time\"  TEXT,\n" +
                        "\"source\"  TEXT,\n" +
                        "PRIMARY KEY (\"id\" ASC)\n" +
                        ");";
                SqliteHelper.executeUpdateForSync(sql);
                String indexsq="CREATE UNIQUE INDEX index_transactionId ON schedule (transaction_id);";
                SqliteHelper.executeUpdateForSync(indexsq);
            }
            boolean exist2 = ScheduleBakDao.existTable();
            if (!exist2) {
                //备份其他节点的任务
                String sql2 = "CREATE TABLE \"schedule_bak\" (\n" +
                        "\"id\"  TEXT NOT NULL,\n" +
                        "\"class_path\"  TEXT,\n" +
                        "\"execute_time\"  INTEGER,\n" +
                        "\"task_type\"  TEXT,\n" +
                        "\"period\"  INTEGER,\n" +
                        "\"unit\"  TEXT,\n" +
                        "\"param\"  TEXT,\n" +
                        "\"transaction_id\"  TEXT,\n" +
                        "\"create_time\"  TEXT,\n" +
                        "\"modify_time\"  TEXT,\n" +
                        "\"source\"  TEXT,\n" +
                        "PRIMARY KEY (\"id\" ASC)\n" +
                        ");";
                SqliteHelper.executeUpdateForSync(sql2);
                String indexsql="CREATE UNIQUE INDEX index_transactionId ON schedule_bak (transaction_id);";
                SqliteHelper.executeUpdateForSync(indexsql);
            }
            boolean exist3 = ScheduleSyncDao.existTable();
            if (!exist3) {
                //本地待运行的任务
                String sql3 = "CREATE TABLE \"schedule_sync\" (\n" +
                        "\"transaction_id\"  TEXT,\n" +
                        "\"schedule_id\"  TEXT NOT NULL,\n" +
                        "\"follow\"  TEXT,\n" +
                        "\"status\"  INTEGER,\n" +
                        "\"create_time\"  TEXT,\n" +
                        "\"modify_time\"  TEXT\n" +
                        ");";
                SqliteHelper.executeUpdateForSync(sql3);
                String indexsql="CREATE INDEX index_scheduleId ON schedule_sync (schedule_id);";
                SqliteHelper.executeUpdateForSync(indexsql);
            }
            boolean exist4 = TransactionLogDao.existTable();
            if (!exist4) {
                //本地待运行的任务
                String sql4 = "CREATE TABLE \"transaction_log\" (\n" +
                        "\"id\"  TEXT NOT NULL,\n" +
                        "\"content\"  TEXT,\n" +
                        "\"table_name\"  TEXT,\n" +
                        "\"type\"  INTEGER,\n" +
                        "\"status\"  INTEGER,\n" +
                        "\"follows\"  TEXT,\n" +
                        "\"retry_time\"  TEXT,\n" +
                        "\"retry_count\"  INTEGER,\n" +
                        "\"create_time\"  TEXT,\n" +
                        "\"modify_time\"  TEXT,\n" +
                        "PRIMARY KEY (\"id\" ASC)\n" +
                        ");";
                SqliteHelper.executeUpdateForSync(sql4);
            }
            hasInit = true;
            log.debug("Sqlite 初始化完成。线程:{}", Thread.currentThread().getId());
            return true;
        } catch (Exception e) {
            log.error("easyTask.db init fail.", e);
            return false;
        }
    }
}
