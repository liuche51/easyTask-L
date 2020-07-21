package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.TransactionLog;
import com.github.liuche51.easyTask.util.DateUtils;
import com.github.liuche51.easyTask.util.StringConstant;
import org.sqlite.SQLiteException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionLogDao {
    /**访问的db名称*/
    private static final String dbName= StringConstant.TRANSACTION_LOG;
    /**可重入锁*/
    private static ReentrantLock lock=new ReentrantLock();
    public static boolean existTable() throws SQLException, ClassNotFoundException {
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT COUNT(*) FROM sqlite_master where type='table' and name='transaction_log';");
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
    public static void saveBatch(List<TransactionLog> transactionLogs) throws Exception {
        if (!DbInit.hasInit)
            DbInit.init();
        transactionLogs.forEach(x->{
            x.setCreateTime(DateUtils.getCurrentDateTime());
            x.setModifyTime(DateUtils.getCurrentDateTime());
            x.setRetryCount(new Short("0"));
            x.setRetryTime(StringConstant.EMPTY);
        });
        String sql = contactSaveSql(transactionLogs);
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void updateStatusById(String id, short status) throws SQLException, ClassNotFoundException {
        String sql = "update transaction_log set status=" + status + ",modify_time='"+DateUtils.getCurrentDateTime()+"' where id='" + id+"';";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void updateStatusByIds(String[] ids, short status) throws SQLException, ClassNotFoundException {
        String str=SqliteHelper.getInConditionStr(ids);
        String sql = "update transaction_log set status=" + status + ",modify_time='"+DateUtils.getCurrentDateTime()+"' where id in" + str+";";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void updateRetryInfoById(String id,short retryCount,String retryTime) throws SQLException, ClassNotFoundException {
        String sql = "update transaction_log set retry_count=" + retryCount + ",modify_time='"+retryTime+"',modify_time='"+DateUtils.getCurrentDateTime()+"' where id='" + id+"';";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static List<TransactionLog> selectByStatusAndType(short status, short type, int count) throws SQLException, ClassNotFoundException {
        List<TransactionLog> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM transaction_log where status = " + status + " and type="+type+" limit " + count + ";");
            while (resultSet.next()) {
                TransactionLog transactionLog = getTransaction(resultSet);
                list.add(transactionLog);
            }
        }catch (SQLiteException e){
            SqliteHelper.writeDatabaseLockedExceptionLog(e,"TransactionLogDao->selectByStatusAndType");
        } finally {
            helper.destroyed();
        }
        return list;
    }
    public static List<TransactionLog> selectByStatusAndType(short[] status, short type, int count) throws SQLException, ClassNotFoundException {
        List<TransactionLog> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            String instr = SqliteHelper.getInConditionStr(status);
            ResultSet resultSet = helper.executeQuery("SELECT * FROM transaction_log where status in " + instr + " and type="+type+" limit " + count + ";");
            while (resultSet.next()) {
                TransactionLog transactionLog = getTransaction(resultSet);
                list.add(transactionLog);
            }
        } catch (SQLiteException e){
            SqliteHelper.writeDatabaseLockedExceptionLog(e,"TransactionLogDao->selectByStatusAndType");
        }finally {
            helper.destroyed();
        }
        return list;
    }
    public static List<TransactionLog> selectByStatusAndReTryCount(short status, short type, short lessThanCancelReTryCount, int count) throws SQLException, ClassNotFoundException {
        List<TransactionLog> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM transaction_log where status = " + status + " and type="+type+" and retry_count<"+lessThanCancelReTryCount+" limit " + count + ";");
            while (resultSet.next()) {
                TransactionLog transactionLog = getTransaction(resultSet);
                list.add(transactionLog);
            }
        }catch (SQLiteException e){
            SqliteHelper.writeDatabaseLockedExceptionLog(e,"TransactionLogDao->selectByStatusAndReTryCount");
        } finally {
            helper.destroyed();
        }
        return list;
    }
    public static List<TransactionLog> selectByTaskId(String taskId) throws SQLException {
        List<TransactionLog> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM transaction_log where content like '%" + taskId + "%';");
            while (resultSet.next()) {
                TransactionLog transactionLog = getTransaction(resultSet);
                list.add(transactionLog);
            }
        }catch (SQLiteException e){
            SqliteHelper.writeDatabaseLockedExceptionLog(e,"TransactionLogDao->selectByTaskId");
        } finally {
            helper.destroyed();
        }
        return list;
    }
    public static void deleteByTypes(short[] types) throws SQLException, ClassNotFoundException {
        String instr=SqliteHelper.getInConditionStr(types);
        String sql = "delete FROM transaction_log where type in "+instr+";";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static void deleteByStatus(short status) throws SQLException, ClassNotFoundException {
        String sql = "delete FROM transaction_log where status = " + status+";";
        SqliteHelper.executeUpdateForSync(sql,dbName,lock);
    }
    public static boolean isExistById(String id) throws SQLException, ClassNotFoundException {
        SqliteHelper helper = new SqliteHelper(dbName);
        try {
            ResultSet resultSet = helper.executeQuery("SELECT count(0) as count FROM transaction_log where id ='"+id+"';");
            while (resultSet.next()) {
                int count = resultSet.getInt("count");
                if(count>0) return true;
            }
        }catch (SQLiteException e){
            SqliteHelper.writeDatabaseLockedExceptionLog(e,"TransactionLogDao->isExistById");
        }
        finally {
            helper.destroyed();
        }
        return false;
    }
    private static TransactionLog getTransaction(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("id");
        String content = resultSet.getString("content");
        String tableName = resultSet.getString("table_name");
        short type = resultSet.getShort("type");
        short status = resultSet.getShort("status");
        String follows = resultSet.getString("follows");
        String retryTime = resultSet.getString("retry_time");
        short retryCount = resultSet.getShort("retry_count");
        String modifyTime = resultSet.getString("modify_time");
        String createTime = resultSet.getString("create_time");
        TransactionLog transactionLog = new TransactionLog();
        transactionLog.setId(id);
        transactionLog.setTableName(tableName);
        transactionLog.setType(type);
        transactionLog.setStatus(status);
        transactionLog.setContent(content);
        transactionLog.setFollows(follows);
        transactionLog.setRetryTime(retryTime);
        transactionLog.setRetryCount(retryCount);
        transactionLog.setModifyTime(modifyTime);
        transactionLog.setCreateTime(createTime);
        return transactionLog;
    }
    private static String contactSaveSql(List<TransactionLog> transactionLogs) {
        StringBuilder sql1 = new StringBuilder("insert into transaction_log(id,content,table_name,type,status,follows,retry_time,retry_count,create_time,modify_time) values");
        for (TransactionLog log : transactionLogs) {
            sql1.append("('");
            sql1.append(log.getId()).append("','");
            sql1.append(log.getContent()).append("',");
            sql1.append(log.getTableName()).append(",'");
            sql1.append(log.getType()).append("',");
            sql1.append(log.getStatus()).append(",'");
            sql1.append(log.getFollows()).append("','");
            sql1.append(log.getRetryTime()).append("',");
            sql1.append(log.getRetryCount()).append(",'");
            sql1.append(log.getCreateTime()).append("','");
            sql1.append(log.getModifyTime()).append("')").append(',');
        }
        String sql = sql1.substring(0, sql1.length() - 1);//去掉最后一个逗号
        return sql.concat(";");
    }
}
