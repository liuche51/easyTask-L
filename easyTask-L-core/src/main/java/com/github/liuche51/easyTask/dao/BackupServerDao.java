package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.core.*;
import com.github.liuche51.easyTask.dto.BackupServer;
import com.github.liuche51.easyTask.dto.Schedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;

class BackupServerDao {
    private static Logger log = LoggerFactory.getLogger(AnnularQueue.class);

    public static boolean existTable() {
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT COUNT(*) FROM sqlite_master where type='table' and name='backup_server';");
            while (resultSet.next()) {
                int count = resultSet.getInt(1);
                if (count > 0)
                    return true;
            }
        } catch (Exception e) {
            log.error("BackupServerDao.existTable Exception:{}", e);
        } finally {
            helper.destroyed();
        }
        return false;
    }

    public static boolean save(BackupServer backupServer) {
        try {
            if (!DbInit.hasInit)
                DbInit.init();
            String sql = "insert into backup_server(id,server,create_time) values('"
                    + backupServer.getId() + "','" + ZonedDateTime.now().toLocalTime() + "');";
            int count = SqliteHelper.executeUpdateForSync(sql);
            if (count > 0) {
                return true;
            }
        } catch (Exception e) {
            log.error("BackupServerDao.save Exception",e);
        }
        return false;
    }

    public static List<BackupServer> selectAll() {
        List<BackupServer> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM backup_server;");
            while (resultSet.next()) {
                try {
                    long id = resultSet.getLong("id");
                    String server = resultSet.getString("server");
                    String createTime = resultSet.getString("create_time");
                    BackupServer backupServer = new BackupServer();
                    backupServer.setId(id);
                    backupServer.setServer(server);
                    backupServer.setCreateTime(createTime);
                    list.add(backupServer);
                } catch (Exception e) {
                    log.error("BackupServerDao.selectAll a item exception:{}", e);
                }
            }
        } catch (Exception e) {
            log.error("BackupServerDao.selectAll exception:{}", e);
        } finally {
            helper.destroyed();
        }
        return list;
    }

    public static boolean delete(long id) {
        try {
            String sql = "delete FROM backup_server where id=" + id + ";";
            int count = SqliteHelper.executeUpdateForSync(sql);
            if (count > 0)
                return true;
        } catch (Exception e) {
            log.error("BackupServerDao.delete Exception:{}", e);
        }
        return false;
    }

    public static int getAllCount() {
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT COUNT(*) FROM backup_server;");
            while (resultSet.next()) {
                return resultSet.getInt(1);
            }
        } catch (Exception e) {
            log.error("BackupServerDao.getAllCount Exception:{}", e);
        } finally {
            helper.destroyed();
        }
        return 0;
    }
}
