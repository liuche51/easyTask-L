package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.core.TaskType;
import com.github.liuche51.easyTask.core.TimeUnit;
import com.github.liuche51.easyTask.util.DateUtils;
import com.github.liuche51.easyTask.util.StringConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ScheduleDao {
    /**访问的db名称*/
    private static final String dbName= StringConstant.SCHEDULE;
    /**可重入锁*/
    private static ReentrantLock lock=new ReentrantLock();
    public static boolean existTable() throws SQLException, ClassNotFoundException {
        SqliteHelper helper = new SqliteHelper(dbName);
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
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }

    public static void saveBatch(List<Schedule> schedules) throws Exception {
        if (!DbInit.hasInit)
            DbInit.init();
        String sql = contactSaveSql(schedules);
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }

    public static List<Schedule> selectAll() throws SQLException, ClassNotFoundException {
        List<Schedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM schedule;");
            while (resultSet.next()) {
                Schedule schedule = getSchedule(resultSet);
                list.add(schedule);
            }
        }catch (SQLiteException e){
            SqliteHelper.writeDatabaseLockedExceptionLog(e,"ScheduleDao->selectAll");
        } finally {
            helper.destroyed();
        }
        return list;
    }

    public static List<Schedule> selectByIds(String[] ids) throws SQLException, ClassNotFoundException {
        List<Schedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            String instr = SqliteHelper.getInConditionStr(ids);
            ResultSet resultSet = helper.executeQuery("SELECT * FROM schedule where id in " + instr + ";");
            while (resultSet.next()) {
                Schedule schedule = getSchedule(resultSet);
                list.add(schedule);
            }
        }catch (SQLiteException e){
            SqliteHelper.writeDatabaseLockedExceptionLog(e,"ScheduleDao->selectByIds");
        } finally {
            helper.destroyed();
        }
        return list;
    }
    public static List<Schedule> selectByTransactionId(String tranId) throws SQLException, ClassNotFoundException {
        List<Schedule> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM schedule where transaction_id = '" + tranId + "';");
            while (resultSet.next()) {
                Schedule schedule = getSchedule(resultSet);
                list.add(schedule);
            }
        }catch (SQLiteException e){
            SqliteHelper.writeDatabaseLockedExceptionLog(e,"ScheduleDao->selectByTransactionId");
        } finally {
            helper.destroyed();
        }
        return list;
    }
    public static void deleteByIds(String[] ids) throws SQLException, ClassNotFoundException {
        String instr=SqliteHelper.getInConditionStr(ids);
        String sql = "delete FROM schedule where id in" + instr + ";";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void deleteByTransactionIds(String[] ids) throws SQLException, ClassNotFoundException {
        String instr=SqliteHelper.getInConditionStr(ids);
        String sql = "delete FROM schedule where transaction_id in" + instr + ";";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void deleteAll() throws SQLException, ClassNotFoundException {
        String sql = "delete FROM schedule;";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }

    public static int getAllCount() throws SQLException, ClassNotFoundException {
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT COUNT(*) FROM schedule;");
            while (resultSet.next()) {
                return resultSet.getInt(1);
            }
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
        String transactionId = resultSet.getString("transaction_id");
        String createTime = resultSet.getString("create_time");
        String modifyTime = resultSet.getString("modify_time");
        String source = resultSet.getString("source");
        Schedule schedule = new Schedule();
        schedule.setId(id);
        schedule.setClassPath(classPath);
        schedule.setExecuteTime(executeTime);
        schedule.setTaskType(taskType);
        schedule.setPeriod(period);
        schedule.setUnit(unit);
        schedule.setParam(param);
        schedule.setTransactionId(transactionId);
        schedule.setCreateTime(createTime);
        schedule.setModifyTime(modifyTime);
        schedule.setSource(source);
        return schedule;
    }

    private static String contactSaveSql(List<Schedule> schedules) {
        StringBuilder sql1 = new StringBuilder("insert into schedule(id,class_path,execute_time,task_type,period,unit,param,transaction_id,create_time,modify_time,source) values");
        for (Schedule schedule : schedules) {
            schedule.setCreateTime(DateUtils.getCurrentDateTime());
            schedule.setModifyTime(DateUtils.getCurrentDateTime());
            sql1.append("('");
            sql1.append(schedule.getId()).append("','");
            sql1.append(schedule.getClassPath()).append("',");
            sql1.append(schedule.getExecuteTime()).append(",'");
            sql1.append(schedule.getTaskType()).append("',");
            sql1.append(schedule.getPeriod()).append(",'");
            sql1.append(schedule.getUnit()).append("','");
            sql1.append(schedule.getParam()).append("','");
            sql1.append(schedule.getTransactionId()).append("','");
            sql1.append(schedule.getCreateTime()).append("','");
            sql1.append(schedule.getModifyTime()).append("','");
            sql1.append(schedule.getSource()).append("')").append(',');
        }
        String sql = sql1.substring(0, sql1.length() - 1);//去掉最后一个逗号
        return sql.concat(";");
    }
}
