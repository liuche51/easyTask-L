package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleSync;
import com.github.liuche51.easyTask.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ScheduleSyncDao {
    private static Logger log = LoggerFactory.getLogger(ScheduleSyncDao.class);

    public static boolean existTable() {
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT COUNT(*) FROM sqlite_master where type='table' and name='schedule_sync';");
            while (resultSet.next()) {
                int count = resultSet.getInt(1);
                if (count > 0)
                    return true;
            }
        } catch (Exception e) {
            log.error("ScheduleSyncDao.existTable Exception:{}", e);
        } finally {
            helper.destroyed();
        }
        return false;
    }

    public static boolean save(ScheduleSync scheduleSync) throws Exception {
        if (!DbInit.hasInit)
            DbInit.init();
        scheduleSync.setCreateTime(DateUtils.getCurrentDateTime());
        scheduleSync.setModifyTime(DateUtils.getCurrentDateTime());
        String sql = "insert into schedule_sync(schedule_id,follow,status,create_time,modify_time) values('"
                + scheduleSync.getScheduleId() + "','" + scheduleSync.getFollow() + "'," + scheduleSync.getStatus()
                + ",'" + scheduleSync.getCreateTime()  + "','" + scheduleSync.getCreateTime() + "');";
        int count = SqliteHelper.executeUpdateForSync(sql);
        if (count > 0) {
            return true;
        }else
            throw new Exception("save result count="+count);
    }

    public static List<ScheduleSync> selectByFollowAndStatusWithCount(String follow, short status, int count) {
        List<ScheduleSync> list = new ArrayList<>(count);
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM schedule_sync where follow='" + follow +
                    "' and status=" + status + " limit " + count + ";");
            while (resultSet.next()) {
                try {
                    String scheduleId1 = resultSet.getString("schedule_id");
                    String follow1 = resultSet.getString("follow");
                    short status1 = resultSet.getShort("status");
                    String createTime = resultSet.getString("create_time");
                    String modifyTime = resultSet.getString("modify_time");
                    ScheduleSync scheduleSync = new ScheduleSync();
                    scheduleSync.setScheduleId(scheduleId1);
                    scheduleSync.setFollow(follow1);
                    scheduleSync.setStatus(status1);
                    scheduleSync.setCreateTime(createTime);
                    scheduleSync.setModifyTime(modifyTime);
                    list.add(scheduleSync);
                } catch (Exception e) {
                    log.error("selectByFollowAndStatusWithCount.selectAll a item exception:{}", e);
                }
            }
        } catch (Exception e) {
            log.error("selectByFollowAndStatusWithCount.selectAll exception:{}", e);
        } finally {
            helper.destroyed();
        }
        return list;
    }

    public static void updateFollowAndStatusByFollow(String oldFollow, String newFollow, short status) throws SQLException, ClassNotFoundException {
        String sql = "update schedule_sync set follow='" + newFollow + "', status=" + status + ",modify_time='"+DateUtils.getCurrentDateTime()+"' where follow='" + oldFollow + "';";
        SqliteHelper.executeUpdateForSync(sql);
    }

    public static void updateStatusByFollowAndStatus(String follow, short status, short updateStatus) throws SQLException, ClassNotFoundException {
        String sql = "update schedule_sync set status=" + updateStatus + ",modify_time='"+DateUtils.getCurrentDateTime()+"' where follow='" + follow + "' and status=" + status + ";";
        SqliteHelper.executeUpdateForSync(sql);
    }

    public static void updateStatusByScheduleIdAndFollow(String scheduleId, String follow, short status) throws SQLException, ClassNotFoundException {
        String sql = "update schedule_sync set status=" + status + ",modify_time='"+DateUtils.getCurrentDateTime()+"' where schedule_id='" + scheduleId + "' and follow='" + follow + "';";
        SqliteHelper.executeUpdateForSync(sql);
    }

    public static void delete(String id) throws SQLException, ClassNotFoundException {
        String sql = "delete FROM schedule where id='" + id + "';";
        SqliteHelper.executeUpdateForSync(sql);
    }
}
