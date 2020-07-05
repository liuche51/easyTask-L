package com.github.liuche51.easyTask.cluster.task.tran;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.leader.DeleteTaskTCC;
import com.github.liuche51.easyTask.cluster.task.TimerTask;
import com.github.liuche51.easyTask.dao.TransactionDao;
import com.github.liuche51.easyTask.dto.Transaction;
import com.github.liuche51.easyTask.enume.TransactionStatusEnum;
import com.github.liuche51.easyTask.enume.TransactionTableEnum;
import com.github.liuche51.easyTask.enume.TransactionTypeEnum;
import com.github.liuche51.easyTask.util.DateUtils;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 任务删除事务重试第一阶段定时任务
 * 只处理事务已经标记为Try的。说明当前事务第一阶段就发生异常，需要重试
 * 通过最大努力通知的方式实现最终一致性
 */
public class RetryDelTransactionTask extends TimerTask {
    @Override
    public void run() {
        List<Transaction> list = null;
        while (!isExit()) {
            List<Transaction> scheduleList = null;
            try {
                list = TransactionDao.selectByStatusAndReTryCount(TransactionStatusEnum.TRIED, TransactionTypeEnum.DELETE, new Short("3"), 100);
                log.info("RetryDelTransactionTask load count="+list.size());
                scheduleList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE.equals(x.getTable())).collect(Collectors.toList());
                if (scheduleList != null && scheduleList.size() > 0) {
                    for (Transaction x : scheduleList) {
                        //如果距离上次重试时间不足5分钟，则跳过重试
                        if (x.getRetryTime() != null && x.getRetryTime() != "") {
                            if (ZonedDateTime.now().minusMinutes(5)
                                    .compareTo(DateUtils.parse(x.getRetryTime())) > 0) {
                                continue;
                            }
                        }
                        try {
                            List<String> cancelFollowsHost = JSONObject.parseObject(x.getFollows(), new TypeReference<List<String>>() {});
                            List<Node> cancelFollows = new ArrayList<>(cancelFollowsHost.size());
                            if (cancelFollowsHost != null) {
                                cancelFollowsHost.forEach(y -> {
                                    String[] hp = y.split(":");
                                    cancelFollows.add(new Node(hp[0], Integer.parseInt(hp[1])));
                                });
                                DeleteTaskTCC.tryDel(x.getId(), x.getContent(), cancelFollows);
                            }
                        } catch (Exception e) {
                            log.error("RetryDelTransactionTask item exception!", e);
                            TransactionDao.updateRetryInfoById(x.getId(), (short) (x.getRetryCount()+1),DateUtils.getCurrentDateTime());
                        }
                    };
                }
            } catch (Exception e) {
                log.error("RetryDelTransactionTask exception!", e);
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
