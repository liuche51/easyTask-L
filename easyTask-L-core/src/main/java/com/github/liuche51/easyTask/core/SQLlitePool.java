package com.github.liuche51.easyTask.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedDeque;

class SQLlitePool {
    final static Logger logger = LoggerFactory.getLogger(SqliteHelper.class);
    private static String driver = "org.sqlite.JDBC";
    private static ConcurrentLinkedDeque<Connection> pool;//支持并发
    private static SQLlitePool singleton = null;

    public static SQLlitePool getInstance() {
        if (singleton == null) {
            synchronized (SQLlitePool.class) {
                if (singleton == null) {
                    singleton = new SQLlitePool();
                }
            }
        }
        return singleton;
    }

    private SQLlitePool() {
    }

    public void init() {
        try {
            if (EasyTaskConfig.getInstance().getTaskStorePath() == null || EasyTaskConfig.getInstance().getTaskStorePath().equals("")) {
                EasyTaskConfig.getInstance().setTaskStorePath(Util.getDefaultDbDirect() + "/easyTask.db");//注意“/”符号目前测试兼容Windows和Linux，不要改成“\”符号不兼容Linux
            }
            logger.debug("dbFilePath:{}", EasyTaskConfig.getInstance().getTaskStorePath());
            Class.forName(driver);
        }catch (Exception e){
            logger.error("sqlite init fail", e);
        }
        //避免重复创建
        if (pool != null && pool.size() > 0)
            return;
        pool = new ConcurrentLinkedDeque<Connection>();
        for (int i = 0; i < EasyTaskConfig.getInstance().getsQLlitePoolSize(); i++) {
            try {
                Connection con = DriverManager.getConnection("jdbc:sqlite:" + EasyTaskConfig.getInstance().getTaskStorePath());
                if (con == null) {
                    logger.debug("数据库连接创建失败，返回null值");
                } else
                    pool.addLast(con);
            } catch (Exception e) {
                logger.error("sqlite init connection create fail", e);
            }
        }
    }

    public Connection getConnection() {
        if (pool.size() > 0 && !(pool.getLast() == null)) {
            Connection conn = pool.poll();
            return conn;
        }
        try {
            Class.forName(driver);
            Connection con = DriverManager.getConnection("jdbc:sqlite:" + EasyTaskConfig.getInstance().getTaskStorePath());
            if (con == null) {
                logger.debug("数据库连接创建失败，返回null值");
            } else
                return con;
        } catch (Exception e) {
            logger.error("sqlite connection create fail", e);
        }
        return null;
    }

    public boolean freeConnection(Connection conn) {
        if (pool.size() < EasyTaskConfig.getInstance().getsQLlitePoolSize() && pool.size() > 0) {
            pool.addLast(conn);
            return true;
        } else {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("Sqlite connection close exception", e);
            }
        }
        return false;
    }
}

