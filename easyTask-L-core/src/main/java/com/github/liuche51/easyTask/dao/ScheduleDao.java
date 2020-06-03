package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.core.TaskType;
import com.github.liuche51.easyTask.core.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;

public class ScheduleDao {
    private static Logger log = LoggerFactory.getLogger(AnnularQueue.class);

    public static boolean existTable() {
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT COUNT(*) FROM sqlite_master where type='table' and name='schedule';");
            while (resultSet.next()) {
                int count = resultSet.getInt(1);
                if (count > 0)
                    return true;
            }
        } catch (Exception e) {
            log.error("ScheduleDao.existTable Exception:{}", e);
        } finally {
            helper.destroyed();
        }
        return false;
    }

    public static boolean save(Schedule schedule) {
        try {
            if (!DbInit.hasInit)
                DbInit.init();
            schedule.setCreateTime(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            String sql = "insert into schedule(id,class_path,execute_time,task_type,period,unit,param,create_time) values('"
                    + schedule.getId() + "','" + schedule.getClassPath() + "'," + schedule.getExecuteTime()
                    + ",'" + schedule.getTaskType() + "'," + schedule.getPeriod() + ",'" + schedule.getUnit()
                    + "','" + schedule.getParam() + "','" + schedule.getCreateTime() + "');";
            int count = SqliteHelper.executeUpdateForSync(sql);
            if (count > 0) {
                log.debug("任务:{} 已经持久化", schedule.getId());
                return true;
            }
        } catch (Exception e) {
            log.error("ScheduleDao.save Exception taskId:"+schedule.getId(), e);
        }
        return false;
    }

    public static List<Schedule> selectAll() {
        List<Schedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM schedule;");
            while (resultSet.next()) {
                try {
                    String id = resultSet.getString("id");
                    String classPath = resultSet.getString("class_path");
                    Long executeTime = resultSet.getLong("execute_time");
                    String taskType = resultSet.getString("task_type");
                    Long period = resultSet.getLong("period");
                    String unit = resultSet.getString("unit");
                    String param = resultSet.getString("param");
                    String createTime=resultSet.getString("create_time");
                    Schedule schedule=new Schedule();
                    schedule.setId(id);
                    schedule.setClassPath(classPath);
                    schedule.setExecuteTime(executeTime);
                    schedule.setTaskType(taskType);
                    schedule.setPeriod(period);
                    schedule.setUnit(unit);
                    schedule.setParam(param);
                    schedule.setCreateTime(createTime);
                    list.add(schedule);
                } catch (Exception e) {
                    log.error("ScheduleDao.selectAll a item exception:{}", e);
                }
            }
        } catch (Exception e) {
            log.error("ScheduleDao.selectAll exception:{}", e);
        } finally {
            helper.destroyed();
        }
        return list;
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
