package com.github.liuche51.easyTask.cluster.task.tran;

import com.github.liuche51.easyTask.cluster.task.TimerTask;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dao.TransactionDao;
import com.github.liuche51.easyTask.dto.Transaction;
import com.github.liuche51.easyTask.enume.TransactionStatusEnum;
import com.github.liuche51.easyTask.enume.TransactionTableEnum;
import com.github.liuche51.easyTask.enume.TransactionTypeEnum;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 删除已提交的任务定时任务
 * 只处理事务已经标记为取消CANCEL状态的。
 */
public class CancelSaveTransactionTask extends TimerTask {
    @Override
    public void run() {
        List<Transaction> list = null;
        while (!isExit()) {
            List<Transaction> scheduleList = null, scheduleBakList = null;
            try {
                list = TransactionDao.selectByStatusAndType(TransactionStatusEnum.CANCEL, TransactionTypeEnum.SAVE,100);
                scheduleList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE.equals(x.getTable())).collect(Collectors.toList());
                scheduleBakList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE_BAK.equals(x.getTable())).collect(Collectors.toList());
                if (scheduleList != null&&scheduleList.size()>0) {
                    String[] scheduleIds=scheduleList.stream().map(Transaction::getContent).toArray(String[]::new);
                    ScheduleDao.deleteByIds(scheduleIds);
                    TransactionDao.updateStatusByIds(scheduleIds,TransactionStatusEnum.FINISHED);
                }
                if (scheduleBakList != null&&scheduleBakList.size()>0) {
                    String[] scheduleBakIds=scheduleList.stream().map(Transaction::getContent).toArray(String[]::new);
                    ScheduleBakDao.deleteByIds(scheduleBakIds);
                    TransactionDao.updateStatusByIds(scheduleBakIds,TransactionStatusEnum.FINISHED);
                }

            } catch (Exception e) {
                log.error("CancelSaveTransactionTask（）：exception!", e);
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
