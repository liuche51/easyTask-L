package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleSync;
import com.github.liuche51.easyTask.util.DateUtils;
import com.github.liuche51.easyTask.util.StringConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ScheduleSyncDao {
    /**访问的db名称*/
    private static final String dbName= StringConstant.SCHEDULE_SYNC;
    /**可重入锁*/
    private static ReentrantLock lock=new ReentrantLock();
    public static boolean existTable() throws SQLException, ClassNotFoundException {
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT COUNT(*) FROM sqlite_master where type='table' and name='schedule_sync';");
            while (resultSet.next()) {
                int count = resultSet.getInt(1);
                if (count > 0)
                    return true;
            }
        } finally {
            helper.destroyed();
        }
        return false;
    }

    public static void save(ScheduleSync scheduleSync) throws Exception {
        scheduleSync.setCreateTime(DateUtils.getCurrentDateTime());
        scheduleSync.setModifyTime(DateUtils.getCurrentDateTime());
        String sql = "insert into schedule_sync(transaction_id,schedule_id,follow,status,create_time,modify_time) values('"
                + scheduleSync.getTransactionId() + "','" + scheduleSync.getScheduleId() +  "','" + scheduleSync.getFollow() + "'," + scheduleSync.getStatus()
                + ",'" + scheduleSync.getCreateTime() + "','" + scheduleSync.getCreateTime() + "');";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }

    public static List<ScheduleSync> selectByFollowAndStatusWithCount(String follow, short status, int count) throws SQLException, ClassNotFoundException {
        List<ScheduleSync> list = new ArrayList<>(count);
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM schedule_sync where follow='" + follow +
                    "' and status=" + status + " limit " + count + ";");
            while (resultSet.next()) {
                ScheduleSync scheduleSync = getScheduleSync(resultSet);
                list.add(scheduleSync);
            }
        }catch (SQLiteException e){
            SqliteHelper.writeDatabaseLockedExceptionLog(e,"ScheduleSyncDao->selectByFollowAndStatusWithCount");
        } finally {
            helper.destroyed();
        }
        return list;
    }
    public static List<ScheduleSync> selectByTaskId(String taskId) throws SQLException, ClassNotFoundException {
        List<ScheduleSync> list = new ArrayList<>(2);
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM schedule_sync where schedule_id='" + taskId + "';");
            while (resultSet.next()) {
                ScheduleSync scheduleSync = getScheduleSync(resultSet);
                list.add(scheduleSync);
            }
        }catch (SQLiteException e){
            SqliteHelper.writeDatabaseLockedExceptionLog(e,"ScheduleSyncDao->selectByTaskId");
        } finally {
            helper.destroyed();
        }
        return list;
    }
    private static ScheduleSync getScheduleSync(ResultSet resultSet) throws SQLException {
        String transactionId = resultSet.getString("transaction_id");
        String scheduleId = resultSet.getString("schedule_id");
        String follow1 = resultSet.getString("follow");
        short status1 = resultSet.getShort("status");
        String createTime = resultSet.getString("create_time");
        String modifyTime = resultSet.getString("modify_time");
        ScheduleSync scheduleSync = new ScheduleSync();
        scheduleSync.setTransactionId(transactionId);
        scheduleSync.setScheduleId(scheduleId);
        scheduleSync.setFollows(follow1);
        scheduleSync.setStatus(status1);
        scheduleSync.setCreateTime(createTime);
        scheduleSync.setModifyTime(modifyTime);
        return scheduleSync;
    }

    public static void updateFollowAndStatusByFollow(String oldFollow, String newFollow, short status) throws SQLException, ClassNotFoundException {
        String sql = "update schedule_sync set follow='" + newFollow + "', status=" + status + ",modify_time='" + DateUtils.getCurrentDateTime() + "' where follow='" + oldFollow + "';";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }

    public static void updateStatusByFollowAndStatus(String follow, short status, short updateStatus) throws SQLException, ClassNotFoundException {
        String sql = "update schedule_sync set status=" + updateStatus + ",modify_time='" + DateUtils.getCurrentDateTime() + "' where follow='" + follow + "' and status=" + status + ";";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }

    public static void updateStatusByFollowAndScheduleIds(String follow, String[] scheduleIds, short updateStatus) throws SQLException, ClassNotFoundException {
        String str = SqliteHelper.getInConditionStr(scheduleIds);
        String sql = "update schedule_sync set status=" + updateStatus + ",modify_time='" + DateUtils.getCurrentDateTime()
                + "' where follow='" + follow + "' and schedule_id in" + str + ";";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }

    public static void updateStatusByScheduleIdAndFollow(String scheduleId, String follow, short status) throws SQLException, ClassNotFoundException {
        String sql = "update schedule_sync set status=" + status + ",modify_time='" + DateUtils.getCurrentDateTime() + "' where schedule_id='" + scheduleId + "' and follow='" + follow + "';";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void updateStatusAndTransactionIdByScheduleId(String scheduleId, short status,String transactionId) throws SQLException, ClassNotFoundException {
        String sql = "update schedule_sync set status=" + status + ",transaction_id='" + transactionId + "',modify_time='" + DateUtils.getCurrentDateTime() + "' where schedule_id='" + scheduleId + "';";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void updateStatusByTransactionIds(String[] transactionIds,short status) throws SQLException, ClassNotFoundException {
        String instr=SqliteHelper.getInConditionStr(transactionIds);
        String sql = "update schedule_sync set status=" + status + ",modify_time='" + DateUtils.getCurrentDateTime() + "' where transaction_id in " + instr + ";";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void deleteByTransactionIdAndFollow(String transactionId, String follow) throws SQLException, ClassNotFoundException {
        String sql = "delete FROM schedule_sync where transaction_id='" + transactionId + "' and follow='" + follow + "';";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void deleteByStatus(short status) throws SQLException, ClassNotFoundException {
        String sql = "delete FROM schedule_sync where status = " + status+";";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void deleteAll() throws SQLException, ClassNotFoundException {
        String sql = "delete FROM schedule_sync;";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
}
