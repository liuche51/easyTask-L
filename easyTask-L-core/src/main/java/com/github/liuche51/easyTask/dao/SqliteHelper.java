package com.github.liuche51.easyTask.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import com.github.liuche51.easyTask.util.StringConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteException;

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
    private String dbName;

    public SqliteHelper(String dbName) {
        this.dbName=dbName;
        connection = SQLliteMultiPool.getInstance().getConnection(dbName);
    }
    /**
     * 修改数据库专用。保证线程互斥使用.以免发生：The database file is locked异常，但是写的过程中同时有
     * 其他读线程，则读线程仍然会发生此异常。但写操作不受影响。另外如果db被外部的其他工具连接，则也会报异常。
     * 相当于多线程使用了。
     * @param sql
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static int executeUpdateForSync(String sql,String dbName,ReentrantLock lock) throws SQLException, ClassNotFoundException {
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            lock.lock();
            return helper.executeUpdate(sql);
        } finally {
            lock.unlock();
            helper.destroyed();
        }

    }
    /**
     * 执行数据库更新sql语句
     *
     * @param sql
     * @return 更新行数
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public int executeUpdate(String sql) throws SQLException, ClassNotFoundException {
        try {
            int c = getStatement().executeUpdate(sql);
            return c;
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
    public ResultSet executeQuery(String sql) throws SQLException {
        resultSet = getStatement().executeQuery(sql);
        return resultSet;
    }

    private Statement getStatement() throws SQLException {
        if (null == statement) statement = connection.createStatement();
        return statement;
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
                SQLliteMultiPool.getInstance().freeConnection(connection,this.dbName);
                connection = null;
            }
        } catch (SQLException e) {
            logger.error("Sqlite数据库关闭时异常", e);
        }
    }

    /**
     * 获取SQL IN 条件的拼接字符串
     * @param params
     * @return
     */
    public static String getInConditionStr(String[] params){
        if(params==null||params.length==0) return StringConstant.EMPTY;
        StringBuilder instr = new StringBuilder("('");
        for (int i = 0; i < params.length; i++) {
            if (i == params.length - 1)//最后一个
                instr.append(params[i]).append("')");
            else
                instr.append(params[i]).append("','");
        }
        return instr.toString();
    }
    /**
     * 获取SQL IN 条件的拼接数值
     * @param params
     * @return
     */
    public static String getInConditionStr(short[] params){
        StringBuilder instr = new StringBuilder("(");
        for (int i = 0; i < params.length; i++) {
            if (i == params.length - 1)//最后一个
                instr.append(params[i]).append(")");
            else
                instr.append(params[i]).append(",");
        }
        return instr.toString();
    }

    /**
     * 数据库被锁异常日志记录处理
     * 写因为查询语句执行时，同时存在写锁，导致数据库被锁的异常。属于正常可接受的错误
     * @param e
     */
    public static void writeDatabaseLockedExceptionLog(SQLiteException e,String methond) throws SQLiteException {
        if(e.getMessage()!=null&&e.getMessage().contains("SQLITE_BUSY"))
            logger.error("normally exception!"+methond+":"+e.getMessage());
        else
            throw e;
    }
}