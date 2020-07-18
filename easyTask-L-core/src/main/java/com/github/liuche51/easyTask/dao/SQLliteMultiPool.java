package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.util.StringConstant;
import com.github.liuche51.easyTask.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 多连接池
 * 单例模式
 */
public class SQLliteMultiPool {
    final static Logger logger = LoggerFactory.getLogger(SqliteHelper.class);
    private static final String driver = "org.sqlite.JDBC";
    private Map<String, ConcurrentLinkedQueue<Connection>> pools = new HashMap<>();
    private static SQLliteMultiPool singleton = null;

    public Map<String, ConcurrentLinkedQueue<Connection>> getPools() {
        return pools;
    }
    public static SQLliteMultiPool getInstance() {
        if (singleton == null) {
            synchronized (SQLliteMultiPool.class) {
                if (singleton == null) {
                    singleton = new SQLliteMultiPool();
                }
            }
        }
        return singleton;
    }

    /**
     * 初始化连接池
     */
    private SQLliteMultiPool() {
        pools.put(StringConstant.SCHEDULE, new ConcurrentLinkedQueue<Connection>());
        pools.put(StringConstant.SCHEDULE_BAK, new ConcurrentLinkedQueue<Connection>());
        pools.put(StringConstant.SCHEDULE_SYNC, new ConcurrentLinkedQueue<Connection>());
        pools.put(StringConstant.TRANSACTION_LOG, new ConcurrentLinkedQueue<Connection>());
        for (int i = 0; i < AnnularQueue.getInstance().getConfig().getsQLlitePoolSize(); i++) {
            Connection con1 = createConnection(StringConstant.SCHEDULE);
            Connection con2 = createConnection(StringConstant.SCHEDULE_BAK);
            Connection con3 = createConnection(StringConstant.SCHEDULE_SYNC);
            Connection con4 = createConnection(StringConstant.TRANSACTION_LOG);
            if (con1 != null)
                pools.get(StringConstant.SCHEDULE).add(con1);
            if (con2 != null)
                pools.get(StringConstant.SCHEDULE_BAK).add(con2);
            if (con3 != null)
                pools.get(StringConstant.SCHEDULE_SYNC).add(con3);
            if (con4 != null)
                pools.get(StringConstant.TRANSACTION_LOG).add(con4);
        }
    }
    /**
     * 创建连接。
     * 创建失败捕获异常
     * @param dbName
     * @return
     */
    private Connection createConnection(String dbName) {
        Connection con = null;
        try {
            //注意“/”符号目前测试兼容Windows和Linux，不要改成“\”符号不兼容Linux
            con = DriverManager.getConnection("jdbc:sqlite:" + AnnularQueue.getInstance().getConfig().getTaskStorePath() + "/" + dbName + ".db");
            if (con == null) {
                throw new Exception("数据库连接创建失败，返回null值");
            }
        } catch (Exception e) {
            logger.error("sqlite init connection create fail", e);
        }
        return con;
    }

    /**
     * 获取连接。
     * 先从连接池获取，如果没有了，则重新创建一个连接
     * @param dbName
     * @return
     */
    public Connection getConnection(String dbName) {
        ConcurrentLinkedQueue<Connection> pool = pools.get(dbName);
        Connection conn = pool.poll();
        if (conn != null)
            return conn;
        conn = createConnection(dbName);
        return conn;
    }

    /**
     * 释放连接。
     * 如果连接池小于设置的数量，则重新放入池中，否则直接关闭连接
     * @param conn
     * @param dbName
     */
    public void freeConnection(Connection conn,String dbName) {
        ConcurrentLinkedQueue<Connection> pool = pools.get(dbName);
        if (pool.size() < AnnularQueue.getInstance().getConfig().getsQLlitePoolSize()) {
            pool.add(conn);
        } else {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("Sqlite connection close exception", e);
            }
        }
    }
}

