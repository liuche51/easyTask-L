package com.github.liuche51.easyTask.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteException;

import java.sql.ResultSet;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;

class ScheduleDao {
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
            String sql = "insert into schedule(id,class_path,execute_time,task_type,period,unit,param,create_time) values('"
                    + schedule.getScheduleExt().getId() + "','" + schedule.getScheduleExt().getTaskClassPath() + "'," + schedule.getEndTimestamp()
                    + ",'" + schedule.getTaskType().name() + "'," + schedule.getPeriod() + ",'" + (schedule.getUnit() == null ? "" : schedule.getUnit().name())
                    + "','" + Schedule.serializeMap(schedule.getParam()) + "','" + ZonedDateTime.now().toLocalTime() + "');";
            int count = SqliteHelper.executeUpdateForSync(sql);
            if (count > 0) {
                log.debug("任务:{} 已经持久化", schedule.getScheduleExt().getId());
                return true;
            }
        } catch (Exception e) {
            log.error("ScheduleDao.save Exception taskId:{} :{}", schedule.getScheduleExt().getId(), e);
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
                    Integer taskType = resultSet.getInt("task_type");
                    Long period = resultSet.getLong("period");
                    String unit = resultSet.getString("unit");
                    String param = resultSet.getString("param");
                    String backup = resultSet.getString("backup");
                    Schedule schedule = new Schedule();
                    schedule.getScheduleExt().setId(id);
                    schedule.getScheduleExt().setTaskClassPath(classPath);
                    schedule.setEndTimestamp(executeTime);
                    schedule.setParam(Schedule.deserializeMap(param));
                    if ("PERIOD".equals(taskType))
                        schedule.setTaskType(TaskType.PERIOD);
                    else if ("ONECE".equals(taskType))
                        schedule.setTaskType(TaskType.ONECE);
                    schedule.setPeriod(period.longValue());
                    switch (unit) {
                        case "DAYS":
                            schedule.setUnit(TimeUnit.DAYS);
                            break;
                        case "HOURS":
                            schedule.setUnit(TimeUnit.HOURS);
                            break;
                        case "MINUTES":
                            schedule.setUnit(TimeUnit.MINUTES);
                            break;
                        case "SECONDS":
                            schedule.setUnit(TimeUnit.SECONDS);
                            break;
                        default:
                            break;
                    }
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
