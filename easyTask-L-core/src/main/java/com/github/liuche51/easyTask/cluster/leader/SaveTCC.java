package com.github.liuche51.easyTask.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.dao.TransactionDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.dto.Transaction;
import com.github.liuche51.easyTask.enume.TransactionStatusEnum;
import com.github.liuche51.easyTask.enume.TransactionTypeEnum;

public class SaveTCC {
    public static void trySave(Task task) throws Exception {
        Schedule schedule = Schedule.valueOf(task);
        Transaction transaction=new Transaction();
        transaction.setId(schedule.getId());
        transaction.setContent(JSONObject.toJSONString(schedule));
        transaction.setStatus(TransactionStatusEnum.STARTED);
        transaction.setType(TransactionTypeEnum.SAVE);
        TransactionDao.save(transaction);
        //数据高可靠分布式存储
        LeaderService.syncPreDataToFollows(schedule);
    }
}
