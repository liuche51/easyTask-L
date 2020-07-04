package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.core.TaskType;
import com.github.liuche51.easyTask.core.TimeUnit;
import com.github.liuche51.easyTask.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class ScheduleDao {
    private static Logger log = LoggerFactory.getLogger(ScheduleDao.class);
    private int count;

    public static boolean existTable() throws SQLException, ClassNotFoundException {
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT COUNT(*) FROM sqlite_master where type='table' and name='schedule';");
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

    public static void save(Schedule schedule) throws SQLException, ClassNotFoundException {
        if (!DbInit.hasInit)
            DbInit.init();
        String sql = contactSaveSql(Arrays.asList(schedule));
        SqliteHelper.executeUpdateForSync(sql);
    }
    public static void saveBatch(List<Schedule> schedules) throws Exception {
        String sql = contactSaveSql(schedules);
        SqliteHelper.executeUpdateForSync(sql);
    }
    public static List<Schedule> selectAll() {
        List<Schedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM schedule;");
            while (resultSet.next()) {
                try {
                    Schedule schedule = getSchedule(resultSet);
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

    public static List<Schedule> selectByIds(String[] ids) {
        List<Schedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper();
        try {
            String instr = SqliteHelper.getInConditionStr(ids);
            ResultSet resultSet = helper.executeQuery("SELECT * FROM schedule where id in " + instr + ";");
            while (resultSet.next()) {
                try {
                    Schedule schedule = getSchedule(resultSet);
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

    public static void delete(String id) throws SQLException, ClassNotFoundException {
        String sql = "delete FROM schedule where id='" + id + "';";
        int count = SqliteHelper.executeUpdateForSync(sql);
        if (count > 0)
            log.debug("任务:{} 已经删除", id);
    }

    public static void deleteAll() throws SQLException, ClassNotFoundException {
        String sql = "delete FROM schedule;";
        SqliteHelper.executeUpdateForSync(sql);
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

    private static Schedule getSchedule(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("id");
        String classPath = resultSet.getString("class_path");
        Long executeTime = resultSet.getLong("execute_time");
        String taskType = resultSet.getString("task_type");
        Long period = resultSet.getLong("period");
        String unit = resultSet.getString("unit");
        String param = resultSet.getString("param");
        String createTime = resultSet.getString("create_time");
        Schedule schedule = new Schedule();
        schedule.setId(id);
        schedule.setClassPath(classPath);
        schedule.setExecuteTime(executeTime);
        schedule.setTaskType(taskType);
        schedule.setPeriod(period);
        schedule.setUnit(unit);
        schedule.setParam(param);
        schedule.setCreateTime(createTime);
        return schedule;
    }
    private static String contactSaveSql(List<Schedule> schedules) {
        StringBuilder sql1 = new StringBuilder("insert into schedule(id,class_path,execute_time,task_type,period,unit,param,create_time) values");
        for (Schedule schedule : schedules) {
            schedule.setCreateTime(DateUtils.getCurrentDateTime());
            sql1.append("('");
            sql1.append(schedule.getId()).append("','");
            sql1.append(schedule.getClassPath()).append("',");
            sql1.append(schedule.getExecuteTime()).append(",'");
            sql1.append(schedule.getTaskType()).append("',");
            sql1.append(schedule.getPeriod()).append(",'");
            sql1.append(schedule.getUnit()).append("','");
            sql1.append(schedule.getParam()).append("','");
            sql1.append(schedule.getCreateTime()).append("')").append(',');
        }
        String sql = sql1.substring(0, sql1.length() - 1);//去掉最后一个逗号
        return sql.concat(";");
    }
}
