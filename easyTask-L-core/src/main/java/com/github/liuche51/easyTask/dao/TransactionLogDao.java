package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.dto.TransactionLog;
import com.github.liuche51.easyTask.util.DateUtils;
import com.github.liuche51.easyTask.util.StringConstant;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class TransactionLogDao {
    public static boolean existTable() throws SQLException, ClassNotFoundException {
        SqliteHelper helper = new SqliteHelper();
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

    public static void save(TransactionLog transactionLog) throws Exception {
        if (!DbInit.hasInit)
            DbInit.init();
        transactionLog.setCreateTime(DateUtils.getCurrentDateTime());
        transactionLog.setModifyTime(DateUtils.getCurrentDateTime());
        transactionLog.setRetryCount(new Short("0"));
        transactionLog.setRetryTime(StringConstant.EMPTY);
        String sql = "insert into transaction_log(id,content,table,type,status,follows,retry_time,retry_count,create_time,modify_time) values('"
                + transactionLog.getId() + "','" + transactionLog.getContent() + "','" + transactionLog.getTable() + "','" + transactionLog.getType()
                + "'," + transactionLog.getStatus() + ",'" + transactionLog.getFollows() + "','" + transactionLog.getRetryTime() + "',"
                + transactionLog.getRetryCount() + ",'" + transactionLog.getCreateTime() + "','" + transactionLog.getModifyTime() + "');";
        SqliteHelper.executeUpdateForSync(sql);
    }
    public static void updateStatusById(String id, short status) throws SQLException, ClassNotFoundException {
        String sql = "update transaction_log set status=" + status + ",modify_time='"+DateUtils.getCurrentDateTime()+"' where id='" + id+"';";
        SqliteHelper.executeUpdateForSync(sql);
    }
    public static void updateStatusByIds(String[] ids, short status) throws SQLException, ClassNotFoundException {
        String str=SqliteHelper.getInConditionStr(ids);
        String sql = "update transaction_log set status=" + status + ",modify_time='"+DateUtils.getCurrentDateTime()+"' where id in" + str+";";
        SqliteHelper.executeUpdateForSync(sql);
    }
    public static void updateRetryInfoById(String id,short retryCount,String retryTime) throws SQLException, ClassNotFoundException {
        String sql = "update transaction_log set retry_count=" + retryCount + ",modify_time='"+retryTime+"',modify_time='"+DateUtils.getCurrentDateTime()+"' where id='" + id+"';";
        SqliteHelper.executeUpdateForSync(sql);
    }
    public static List<TransactionLog> selectByStatusAndType(short status, short type, int count) throws SQLException, ClassNotFoundException {
        List<TransactionLog> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM transaction_log where status = " + status + " and type="+type+" limit " + count + ";");
            while (resultSet.next()) {
                TransactionLog transactionLog = getTransaction(resultSet);
                list.add(transactionLog);
            }
        } finally {
            helper.destroyed();
        }
        return list;
    }
    public static List<TransactionLog> selectByStatusAndType(short[] status, short type, int count) throws SQLException, ClassNotFoundException {
        List<TransactionLog> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper();
        try {
            String instr = SqliteHelper.getInConditionStr(status);
            ResultSet resultSet = helper.executeQuery("SELECT * FROM transaction_log where status in " + instr + " and type="+type+" limit " + count + ";");
            while (resultSet.next()) {
                TransactionLog transactionLog = getTransaction(resultSet);
                list.add(transactionLog);
            }
        } finally {
            helper.destroyed();
        }
        return list;
    }
    public static List<TransactionLog> selectByStatusAndReTryCount(short status, short type, short lessThanCancelReTryCount, int count) throws SQLException, ClassNotFoundException {
        List<TransactionLog> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM transaction_log where status = " + status + " and type="+type+" and retry_count<"+lessThanCancelReTryCount+" limit " + count + ";");
            while (resultSet.next()) {
                TransactionLog transactionLog = getTransaction(resultSet);
                list.add(transactionLog);
            }
        } finally {
            helper.destroyed();
        }
        return list;
    }
    public static void deleteByStatus(short status) throws SQLException, ClassNotFoundException {
        String sql = "delete FROM transaction_log where status = " + status+";";
        SqliteHelper.executeUpdateForSync(sql);
    }

    public static void deleteByTypes(short[] types) throws SQLException, ClassNotFoundException {
        String instr=SqliteHelper.getInConditionStr(types);
        String sql = "delete FROM transaction_log where type in "+instr+";";
        SqliteHelper.executeUpdateForSync(sql);
    }
    private static TransactionLog getTransaction(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("id");
        String content = resultSet.getString("content");
        String table = resultSet.getString("table");
        short type = resultSet.getShort("type");
        short status = resultSet.getShort("status");
        String follows = resultSet.getString("follows");
        String retryTime = resultSet.getString("retry_time");
        short retryCount = resultSet.getShort("retry_count");
        String modifyTime = resultSet.getString("modify_time");
        String createTime = resultSet.getString("create_time");
        TransactionLog transactionLog = new TransactionLog();
        transactionLog.setId(id);
        transactionLog.setTable(table);
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
}
