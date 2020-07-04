package com.github.liuche51.easyTask.cluster.task;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dao.TransactionDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.Transaction;
import com.github.liuche51.easyTask.enume.TransactionStatusEnum;
import com.github.liuche51.easyTask.enume.TransactionTableEnum;

import java.util.List;
import java.util.stream.Collectors;

public class SubmitTransactionTask extends TimerTask {
    @Override
    public void run() {
        List<Transaction> list = null;
        while (!isExit()) {
            List<Transaction> scheduleList = null, scheduleBakList = null;
            List<Schedule> scheduleList1 = null;
            List<ScheduleBak> scheduleBakList1 = null;
            try {
                list = TransactionDao.selectByStatus(TransactionStatusEnum.CONFIRM,100);
                scheduleList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE.equals(x.getTable())).collect(Collectors.toList());
                scheduleBakList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE_BAK.equals(x.getTable())).collect(Collectors.toList());
                if (scheduleList != null&&scheduleList.size()>0) {
                    scheduleList.forEach(x->{
                        scheduleList1.add(JSONObject.parseObject(x.getContent(),Schedule.class));
                    });
                    ScheduleDao.saveBatch(scheduleList1);
                    String[] scheduleIds=scheduleList.stream().map(Transaction::getId).toArray(String[]::new);
                    TransactionDao.updateStatusByIds(scheduleIds,TransactionStatusEnum.FINISHED);
                }
                if (scheduleBakList != null&&scheduleBakList.size()>0) {
                    scheduleBakList.forEach(x->{
                        scheduleBakList1.add(JSONObject.parseObject(x.getContent(),ScheduleBak.class));
                    });
                    ScheduleBakDao.saveBatch(scheduleBakList1);
                    String[] scheduleBakIds=scheduleList.stream().map(Transaction::getId).toArray(String[]::new);
                    TransactionDao.updateStatusByIds(scheduleBakIds,TransactionStatusEnum.FINISHED);
                }

            } catch (Exception e) {
                log.error("", e);
            }
            try {
                if (list == null || list.size() == 0)//防止频繁空转
                    Thread.sleep(500);
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }
}
