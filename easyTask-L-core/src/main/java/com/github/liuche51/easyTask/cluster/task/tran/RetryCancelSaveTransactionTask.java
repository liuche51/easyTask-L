package com.github.liuche51.easyTask.cluster.task.tran;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.leader.SaveTaskTCC;
import com.github.liuche51.easyTask.cluster.task.TimerTask;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dao.TransactionLogDao;
import com.github.liuche51.easyTask.dto.TransactionLog;
import com.github.liuche51.easyTask.enume.TransactionStatusEnum;
import com.github.liuche51.easyTask.enume.TransactionTableEnum;
import com.github.liuche51.easyTask.enume.TransactionTypeEnum;
import com.github.liuche51.easyTask.util.DateUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 重试取消任务提交定时任务
 * 获取事务标记为CANCEL状态的。重试多次取消任务提交
 */
public class RetryCancelSaveTransactionTask extends TimerTask {
    @Override
    public void run() {
        List<TransactionLog> list = null;
        while (!isExit()) {
            setLastRunTime(new Date());
            List<TransactionLog> scheduleList = null, scheduleBakList = null;
            try {
                list = TransactionLogDao.selectByStatusAndReTryCount(TransactionStatusEnum.CANCEL, TransactionTypeEnum.SAVE, new Short("3"), 100);
                log.debug("RetryCancelSaveTransactionTask() load count=" + list.size());
                scheduleList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE.equals(x.getTable())).collect(Collectors.toList());
                scheduleBakList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE_BAK.equals(x.getTable())).collect(Collectors.toList());
                if (scheduleList != null && scheduleList.size() > 0) {
                    String[] scheduleTranIds = scheduleList.stream().map(TransactionLog::getId).toArray(String[]::new);
                    ScheduleDao.deleteByTransactionIds(scheduleTranIds);//先清掉自己已经提交的事务
                    for (TransactionLog x : scheduleList) {
                        try {
                            //如果距离上次重试时间不足5分钟，则跳过重试
                            if (x.getRetryTime() != null && x.getRetryTime() != "") {
                                if (ZonedDateTime.now().minusMinutes(5)
                                        .compareTo(DateUtils.parse(x.getRetryTime())) > 0) {
                                    continue;
                                }
                            }
                            List<String> cancelFollowsHost = JSONObject.parseObject(x.getFollows(), new TypeReference<List<String>>() {});
                            List<Node> cancelFollows = new ArrayList<>(cancelFollowsHost.size());
                            if (cancelFollowsHost != null) {
                                cancelFollowsHost.forEach(y -> {
                                    String[] hp = y.split(":");
                                    cancelFollows.add(new Node(hp[0], Integer.parseInt(hp[1])));
                                });
                                log.info("RetryDelTransactionTask()->retryCancel():transactionId="+x.getId()+" retryCount="+x.getRetryCount()+",retryTime="+x.getRetryTime());
                                SaveTaskTCC.retryCancel(x.getId(), cancelFollows);
                            }
                            TransactionLogDao.updateStatusById(x.getId(), TransactionStatusEnum.FINISHED);
                        } catch (Exception e) {
                            log.error("RetryCancelSaveTransactionTask() item exception!", e);
                        }
                    }
                }
                if (scheduleBakList != null && scheduleBakList.size() > 0) {
                    String[] scheduleBakIds = scheduleList.stream().map(TransactionLog::getId).toArray(String[]::new);
                    ScheduleDao.deleteByTransactionIds(scheduleBakIds);
                    TransactionLogDao.updateStatusByIds(scheduleBakIds, TransactionStatusEnum.FINISHED);
                }

            } catch (Exception e) {
                log.error("RetryCancelSaveTransactionTask() exception!", e);
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
