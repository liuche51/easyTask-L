package com.github.liuche51.easyTask.cluster.task.tran;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.cluster.task.TimerTask;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dao.ScheduleSyncDao;
import com.github.liuche51.easyTask.dao.TransactionDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.ScheduleSync;
import com.github.liuche51.easyTask.dto.Transaction;
import com.github.liuche51.easyTask.enume.ScheduleSyncStatusEnum;
import com.github.liuche51.easyTask.enume.TransactionStatusEnum;
import com.github.liuche51.easyTask.enume.TransactionTableEnum;
import com.github.liuche51.easyTask.enume.TransactionTypeEnum;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 任务删除事务批量提交定时任务
 * 只处理事务已经表记为删除确认的。如果leader自己被标记为确认了，那么就可以认为其follow也已经被标记成确认提交事务了
 */
public class CommitDelTransactionTask extends TimerTask {
    @Override
    public void run() {
        List<Transaction> list = null;
        while (!isExit()) {
            List<Transaction> scheduleList = null, scheduleBakList = null;
            try {
                list = TransactionDao.selectByStatusAndType(new short[]{TransactionStatusEnum.CONFIRM,TransactionStatusEnum.TRIED}, TransactionTypeEnum.DELETE,100);
                //对于leader来说，只能处理被标记为CONFIRM的事务。TRIED表示还需要重试通知follow标记删除TRIED状态
                scheduleList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE.equals(x.getTable())&&TransactionStatusEnum.CONFIRM==x.getStatus()).collect(Collectors.toList());
                //对于follow来说。只需要事务被标记为TRIED状态，就可以执行删除操作了
                scheduleBakList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE_BAK.equals(x.getTable())).collect(Collectors.toList());
                if (scheduleList != null&&scheduleList.size()>0) {
                    String[] scheduleIds=scheduleList.stream().map(Transaction::getContent).toArray(String[]::new);
                    ScheduleDao.deleteByIds(scheduleIds);
                    TransactionDao.updateStatusByIds(scheduleIds,TransactionStatusEnum.FINISHED);
                    ScheduleSyncDao.updateStatusByTransactionIds(scheduleIds, ScheduleSyncStatusEnum.DELETED);
                }
                if (scheduleBakList != null&&scheduleBakList.size()>0) {
                    String[] scheduleBakIds=scheduleList.stream().map(Transaction::getContent).toArray(String[]::new);
                    ScheduleBakDao.deleteByIds(scheduleBakIds);
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
