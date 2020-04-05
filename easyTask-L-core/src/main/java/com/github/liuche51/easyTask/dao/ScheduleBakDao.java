package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.core.*;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;

class ScheduleBakDao {
    private static Logger log = LoggerFactory.getLogger(AnnularQueue.class);

    public static boolean existTable() {
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT COUNT(*) FROM sqlite_master where type='table' and name='schedule_bak';");
            while (resultSet.next()) {
                int count = resultSet.getInt(1);
                if (count > 0)
                    return true;
            }
        } catch (Exception e) {
            log.error("ScheduleBakDao.existTable Exception:{}", e);
        } finally {
            helper.destroyed();
        }
        return false;
    }

    public static boolean save(ScheduleBak scheduleBak) {
        try {
            if (!DbInit.hasInit)
                DbInit.init();
            String sql = "insert into schedule(id,class_path,execute_time,task_type,period,unit,param,backup,source,create_time) values('"
                    + scheduleBak.getId() + "','" + scheduleBak.getClassPath() + "'," + scheduleBak.getExecuteTime()
                    + ",'" + scheduleBak.getTaskType() + "'," + scheduleBak.getPeriod() + ",'" + scheduleBak.getUnit()
                    + "','" + scheduleBak.getParam() + "','"+ scheduleBak.getBackup() + ",'" + scheduleBak.getSource() + ",'"
                    + ZonedDateTime.now().toLocalTime() + "');";
            int count = SqliteHelper.executeUpdateForSync(sql);
            if (count > 0) {
                log.debug("任务:{} 已经持久化", scheduleBak.getId());
                return true;
            }
        } catch (Exception e) {
            log.error("ScheduleBakDao.save Exception taskId:{} :{}", scheduleBak.getId(), e);
        }
        return false;
    }
    public static boolean delete(String id) {
        try {
            String sql = "delete FROM schedule where id='" + id + "';";
            int count = SqliteHelper.executeUpdateForSync(sql);
            if (count > 0)
                log.debug("任务:{} 已经删除", id);
        } catch (Exception e) {
            log.error("ScheduleDao.delete Exception:{}", e);
            return false;
        }
        return true;
    }

    public static int getAllCount() {
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT COUNT(*) FROM schedule;");
            while (resultSet.next()) {
                return resultSet.getInt(1);
            }
        } catch (Exception e) {
            log.error("ScheduleDao.getAllCount Exception:{}", e);
        } finally {
            helper.destroyed();
        }
        return 0;
    }
}
