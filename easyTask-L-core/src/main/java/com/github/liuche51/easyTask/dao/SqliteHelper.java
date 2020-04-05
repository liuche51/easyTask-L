package com.github.liuche51.easyTask.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * sqlite帮助类，直接创建该类示例，并调用相应的借口即可对sqlite数据库进行操作
 * <p>
 * 本类基于 sqlite jdbc v56
 *
 * @author haoqipeng
 */
class SqliteHelper {
    final static Logger logger = LoggerFactory.getLogger(SqliteHelper.class);

    private Connection connection;
    private Statement statement;
    private ResultSet resultSet;

    public SqliteHelper() {
        connection = SQLlitePool.getInstance().getConnection();
    }

    /**
     * 执行数据库更新sql语句
     *
     * @param sql
     * @return 更新行数
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private int executeUpdate(String sql) throws SQLException, ClassNotFoundException {
        try {
            int c = getStatement().executeUpdate(sql);
            return c;
        } finally {
            destroyed();
        }

    }

    /**
     * 执行多个sql更新语句
     *
     * @param sqls
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private void executeUpdate(String... sqls) throws SQLException, ClassNotFoundException {
        try {
            for (String sql : sqls) {
                getStatement().executeUpdate(sql);
            }
        } finally {
            destroyed();
        }
    }

    /**
     * 执行数据库更新 sql List
     *
     * @param sqls sql列表
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private void executeUpdate(List<String> sqls) throws SQLException, ClassNotFoundException {
        try {
            for (String sql : sqls) {
                getStatement().executeUpdate(sql);
            }
        } finally {
            destroyed();
        }
    }

    /**
     * 执行sql查询
     *
     * @param sql sql select 语句
     * @return 查询结果
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public ResultSet executeQuery(String sql) throws SQLException, ClassNotFoundException {
        resultSet = getStatement().executeQuery(sql);
        return resultSet;
    }

    private Statement getStatement() throws SQLException, ClassNotFoundException {
        if (null == statement) statement = connection.createStatement();
        return statement;
    }

    /**
     * 修改数据库专用。保证单互斥使用.以免发生：The database file is locked异常，但是写的过程中同时有
     * 其他读线程，则读线程仍然会发生此异常。但写操作不受影响。
     * @param sql
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static synchronized int executeUpdateForSync(String sql) throws SQLException, ClassNotFoundException {
        SqliteHelper helper = new SqliteHelper();
        try {
            return helper.executeUpdate(sql);
        } catch (SQLException e) {
            throw e;
        } finally {
            helper.destroyed();
        }

    }

    /**
     * 数据库资源关闭和释放
     */
    public void destroyed() {
        try {
            if (null != resultSet) {
                resultSet.close();
                resultSet = null;
            }
            if (null != statement) {
                statement.close();
                statement = null;
            }
            if (null != connection) {
                SQLlitePool.getInstance().freeConnection(connection);
                connection = null;
            }
        } catch (SQLException e) {
            logger.error("Sqlite数据库关闭时异常", e);
        }
    }
}