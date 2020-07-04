package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleSync;
import com.github.liuche51.easyTask.dto.Transaction;
import com.github.liuche51.easyTask.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TransactionDao {
    private static Logger log = LoggerFactory.getLogger(TransactionDao.class);

    public static boolean existTable() throws SQLException, ClassNotFoundException {
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT COUNT(*) FROM sqlite_master where type='table' and name='transaction';");
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

    public static void save(Transaction transaction) throws Exception {
        if (!DbInit.hasInit)
            DbInit.init();
        transaction.setCreateTime(DateUtils.getCurrentDateTime());
        transaction.setModifyTime(DateUtils.getCurrentDateTime());
        String sql = "insert into transaction(id,content,type,status,cancel_host,create_time,modify_time) values('"
                + transaction.getId() + "','" + transaction.getContent() + "','" + transaction.getContent()
                + "'," + transaction.getStatus() + ",'" + transaction.getCancelHost() + "','" + transaction.getCreateTime()
                + "','" + transaction.getCreateTime() + "');";
        SqliteHelper.executeUpdateForSync(sql);
    }
    public static void updateStatusById(String id, short status) throws SQLException, ClassNotFoundException {
        String sql = "update transaction set status=" + status + ",modify_time='"+DateUtils.getCurrentDateTime()+"' where id='" + id+"';";
        SqliteHelper.executeUpdateForSync(sql);
    }
    public static void updateStatusByIds(String[] ids, short status) throws SQLException, ClassNotFoundException {
        String str=SqliteHelper.getInConditionStr(ids);
        String sql = "update transaction set status=" + status + ",modify_time='"+DateUtils.getCurrentDateTime()+"' where id in" + str+";";
        SqliteHelper.executeUpdateForSync(sql);
    }
    public static List<Transaction> selectByIds(String[] ids) throws SQLException, ClassNotFoundException {
        List<Transaction> list = new ArrayList<>(ids.length);
        SqliteHelper helper = new SqliteHelper();
        try {
            String instr = SqliteHelper.getInConditionStr(ids);
            ResultSet resultSet = helper.executeQuery("SELECT * FROM transaction where id in " + instr + ";");
            while (resultSet.next()) {
                Transaction transaction = getTransaction(resultSet);
                list.add(transaction);
            }
        } finally {
            helper.destroyed();
        }
        return list;
    }
    public static List<Transaction> selectByStatus(short status,int count) throws SQLException, ClassNotFoundException {
        List<Transaction> list = new LinkedList<>();
        SqliteHelper helper = new SqliteHelper();
        try {
            ResultSet resultSet = helper.executeQuery("SELECT * FROM transaction where status = " + status + " limit " + count + ";");
            while (resultSet.next()) {
                Transaction transaction = getTransaction(resultSet);
                list.add(transaction);
            }
        } finally {
            helper.destroyed();
        }
        return list;
    }
    private static Transaction getTransaction(ResultSet resultSet) throws SQLException {
        String id = resultSet.getString("id");
        String content = resultSet.getString("content");
        String table = resultSet.getString("table");
        short type = (short) resultSet.getInt("type");
        short status = (short) resultSet.getInt("status");
        String cancelHost = resultSet.getString("cancel_host");
        String modifyTime = resultSet.getString("modify_time");
        String createTime = resultSet.getString("create_time");
        Transaction transaction = new Transaction();
        transaction.setId(id);
        transaction.setTable(table);
        transaction.setType(type);
        transaction.setStatus(status);
        transaction.setContent(content);
        transaction.setCancelHost(content);
        transaction.setModifyTime(modifyTime);
        transaction.setCreateTime(createTime);
        return transaction;
    }
}
