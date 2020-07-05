package com.github.liuche51.easyTask.cluster.task.tran;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.cluster.task.TimerTask;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dao.TransactionLogDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.TransactionLog;
import com.github.liuche51.easyTask.enume.TransactionStatusEnum;
import com.github.liuche51.easyTask.enume.TransactionTableEnum;
import com.github.liuche51.easyTask.enume.TransactionTypeEnum;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 新增任务提交定时任务
 * 定时获取已经表记为CONFIRM的事务，并提交至数据表
 */
public class CommitSaveTransactionTask extends TimerTask {
    @Override
    public void run() {
        List<TransactionLog> list = null;
        while (!isExit()) {
            setLastRunTime(new Date());
            List<TransactionLog> scheduleList = null, scheduleBakList = null;
            List<Schedule> scheduleList1 = null;
            List<ScheduleBak> scheduleBakList1 = null;
            try {
                list = TransactionLogDao.selectByStatusAndType(TransactionStatusEnum.CONFIRM, TransactionTypeEnum.SAVE,100);
                scheduleList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE.equals(x.getTableName())).collect(Collectors.toList());
                scheduleBakList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE_BAK.equals(x.getTableName())).collect(Collectors.toList());
                if (scheduleList != null&&scheduleList.size()>0) {
                    scheduleList.forEach(x->{
                        scheduleList1.add(JSONObject.parseObject(x.getContent(),Schedule.class));
                    });
                    ScheduleDao.saveBatch(scheduleList1);
                    String[] scheduleIds=scheduleList.stream().map(TransactionLog::getId).toArray(String[]::new);
                    TransactionLogDao.updateStatusByIds(scheduleIds,TransactionStatusEnum.FINISHED);
                }
                if (scheduleBakList != null&&scheduleBakList.size()>0) {
                    scheduleBakList.forEach(x->{
                        scheduleBakList1.add(JSONObject.parseObject(x.getContent(),ScheduleBak.class));
                    });
                    ScheduleBakDao.saveBatch(scheduleBakList1);
                    String[] scheduleBakIds=scheduleList.stream().map(TransactionLog::getId).toArray(String[]::new);
                    TransactionLogDao.updateStatusByIds(scheduleBakIds,TransactionStatusEnum.FINISHED);
                }

            } catch (Exception e) {
                log.error("CommitSaveTransactionTask():exception!", e);
            }
            try {
                if (new Date().getTime()-getLastRunTime().getTime()<500)//防止频繁空转
                    Thread.sleep(500);
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }
}
