package com.github.liuche51.easyTask.dao;

import com.github.liuche51.easyTask.dto.ScheduleSync;
import com.github.liuche51.easyTask.dto.Transaction;
import com.github.liuche51.easyTask.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
        String sql = "insert into transaction(id,content,type,status,create_time,modify_time) values('"
                + transaction.getId() + "','" + transaction.getContent()  + "','" + transaction.getContent()
                + "'," +transaction.getStatus() + ",'" + transaction.getCreateTime()  + "','" + transaction.getCreateTime() + "');";
        SqliteHelper.executeUpdateForSync(sql);
    }
}
