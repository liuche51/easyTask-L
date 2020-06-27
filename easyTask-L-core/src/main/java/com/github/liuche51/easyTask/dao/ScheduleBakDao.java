package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.core.*;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleBak;
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

public class ScheduleBakDao {
    private static Logger log = LoggerFactory.getLogger(ScheduleBakDao.class);

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
        String sql = "";
        try {
            if (!DbInit.hasInit)
                DbInit.init();
            scheduleBak.setCreateTime(DateUtils.getCurrentDateTime());
            sql = contactSaveSql(Arrays.asList(scheduleBak));
            int count = SqliteHelper.executeUpdateForSync(sql);
            if (count > 0) {
                log.debug("任务:{} 已经持久化", scheduleBak.getId());
                return true;
            }
        } catch (Exception e) {
            log.info("SQL:" + sql);
            log.error("ScheduleBakDao.save Exception taskId:" + scheduleBak.getId(), e);
        }
        return false;
    }

    public static void saveBatch(List<ScheduleBak> scheduleBaks) throws Exception {
        String sql = contactSaveSql(scheduleBaks);
        int count = SqliteHelper.executeUpdateForSync(sql);
        if (count <= 0) {
            throw new Exception("saveBatch result count=" + count);
        }
    }

    private static String contactSaveSql(List<ScheduleBak> scheduleBaks) {
        StringBuilder sql1 = new StringBuilder("insert into schedule_bak(id,class_path,execute_time,task_type,period,unit,param,source,create_time) values");
        for (ScheduleBak scheduleBak : scheduleBaks) {
            scheduleBak.setCreateTime(DateUtils.getCurrentDateTime());
            sql1.append("('");
            sql1.append(scheduleBak.getId()).append("','");
            sql1.append(scheduleBak.getClassPath()).append("',");
            sql1.append(scheduleBak.getExecuteTime()).append(",'");
            sql1.append(scheduleBak.getTaskType()).append("',");
            sql1.append(scheduleBak.getPeriod()).append(",'");
            sql1.append(scheduleBak.getUnit()).append("','");
            sql1.append(scheduleBak.getParam()).append("','");
            sql1.append(scheduleBak.getSource()).append("','");
            sql1.append(scheduleBak.getCreateTime()).append("')").append(',');
        }
        String sql = sql1.substring(0, sql1.length() - 1);//去掉最后一个逗号
        return sql.concat(";");
    }

    public static void delete(String id) throws SQLException, ClassNotFoundException {
        String sql = "delete FROM schedule_bak where id='" + id + "';";
        SqliteHelper.executeUpdateForSync(sql);
    }
    public static void deleteBySource(String source) throws SQLException, ClassNotFoundException {
        String sql = "delete FROM schedule_bak where source='" + source + "';";
        SqliteHelper.executeUpdateForSync(sql);
    }
    public static void deleteBySources(String[] sources) throws SQLException, ClassNotFoundException {
       String conditionStr=SqliteHelper.getInConditionStr(sources);
        String sql = "delete FROM schedule_bak where source not in" + conditionStr + ";";
        SqliteHelper.executeUpdateForSync(sql);
    }
    public static List<ScheduleBak> getBySourceWithCount(String source, int count) throws SQLException, ClassNotFoundException {
        List<ScheduleBak> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM schedule_bak where source='" + source + "' limit " + count + ";");
            while (resultSet.next()) {
                String id = resultSet.getString("id");
                String classPath = resultSet.getString("class_path");
                Long executeTime = resultSet.getLong("execute_time");
                String taskType = resultSet.getString("task_type");
                Long period = resultSet.getLong("period");
                String unit = resultSet.getString("unit");
                String param = resultSet.getString("param");
                String source1 = resultSet.getString("source");
                String createTime = resultSet.getString("create_time");
                ScheduleBak schedulebak = new ScheduleBak();
                schedulebak.setId(id);
                schedulebak.setClassPath(classPath);
                schedulebak.setExecuteTime(executeTime);
                schedulebak.setTaskType(taskType);
                schedulebak.setPeriod(period);
                schedulebak.setUnit(unit);
                schedulebak.setParam(param);
                schedulebak.setSource(source1);
                schedulebak.setCreateTime(createTime);
                list.add(schedulebak);
            }
        } finally {
            helper.destroyed();
        }
        return list;
    }

}
