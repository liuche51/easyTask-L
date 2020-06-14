package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.core.*;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;

public class ScheduleBakDao {
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
        String sql="";
        try {
            if (!DbInit.hasInit)
                DbInit.init();
            scheduleBak.setCreateTime(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            sql = "insert into schedule_bak(id,class_path,execute_time,task_type,period,unit,param,source,create_time) values('"
                    + scheduleBak.getId() + "','" + scheduleBak.getClassPath() + "'," + scheduleBak.getExecuteTime()
                    + ",'" + scheduleBak.getTaskType() + "'," + scheduleBak.getPeriod() + ",'" + scheduleBak.getUnit()
                    + "','" + scheduleBak.getParam()  + "','" + scheduleBak.getSource()+ "','"+ scheduleBak.getCreateTime() + "');";
            int count = SqliteHelper.executeUpdateForSync(sql);
            if (count > 0) {
                log.debug("任务:{} 已经持久化", scheduleBak.getId());
                return true;
            }
        } catch (Exception e) {
            log.info("SQL:"+ sql);
            log.error("ScheduleBakDao.save Exception taskId:"+ scheduleBak.getId(), e);
        }
        return false;
    }
    public static boolean delete(String id) {
        try {
            String sql = "delete FROM schedule_bak where id='" + id + "';";
            int count = SqliteHelper.executeUpdateForSync(sql);
            if (count > 0)
                log.debug("任务:{} 已经删除", id);
        } catch (Exception e) {
            log.error("ScheduleDao.delete Exception:{}", e);
            return false;
        }
        return true;
    }
    public static List<ScheduleBak> getBySource(String source) {
        List<ScheduleBak> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM schedule_bak where source='" + source + "';");
            while (resultSet.next()) {
                try {
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
                } catch (Exception e) {
                    log.error("ScheduleDao.selectAll a item exception:{}", e);
                }
            }
        } catch (Exception e) {
            log.error("ScheduleBakDao.getAllCount Exception:{}", e);
        } finally {
            helper.destroyed();
        }
        return list;
    }

}
