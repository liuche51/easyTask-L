package com.github.liuche51.easyTask.cluster.task.tran;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.leader.SaveTCC;
import com.github.liuche51.easyTask.cluster.task.TimerTask;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dao.TransactionDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.Transaction;
import com.github.liuche51.easyTask.enume.TransactionStatusEnum;
import com.github.liuche51.easyTask.enume.TransactionTableEnum;
import com.github.liuche51.easyTask.enume.TransactionTypeEnum;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RollBackTransactionTask extends TimerTask {
    @Override
    public void run() {
        List<Transaction> list = null;
        while (!isExit()) {
            List<Transaction> scheduleList = null, scheduleBakList = null;
            try {
                list = TransactionDao.selectByStatusAndReTryCount(TransactionStatusEnum.CANCEL, TransactionTypeEnum.SAVE,new Short("3"),100);
                scheduleList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE.equals(x.getTable())).collect(Collectors.toList());
                scheduleBakList = list.stream().filter(x -> TransactionTableEnum.SCHEDULE_BAK.equals(x.getTable())).collect(Collectors.toList());
                if (scheduleList != null&&scheduleList.size()>0) {
                    String[] scheduleTranIds=scheduleList.stream().map(Transaction::getId).toArray(String[]::new);
                    ScheduleDao.deleteByTransactionIds(scheduleTranIds);//先清掉自己已经提交的事务
                    scheduleList.forEach(x->{
                        try {
                            List<String> cancelFollowsHost=JSONObject.parseObject(x.getFollows(),new TypeReference<List<String>>() {});
                            List<Node> cancelFollows=new ArrayList<>(cancelFollowsHost.size());
                            if(cancelFollowsHost!=null){
                                cancelFollowsHost.forEach(y->{
                                    String[] hp=y.split(":");
                                    cancelFollows.add(new Node(hp[0],Integer.parseInt(hp[1])));
                                });
                                SaveTCC.retryCancel(x.getId(),cancelFollows);
                            }
                            TransactionDao.updateStatusById(x.getId(),TransactionStatusEnum.FINISHED);
                        }catch (Exception e){
                            log.error("",e);
                        }
                    });
                }
                if (scheduleBakList != null&&scheduleBakList.size()>0) {
                    String[] scheduleBakIds=scheduleList.stream().map(Transaction::getId).toArray(String[]::new);
                    ScheduleDao.deleteByTransactionIds(scheduleBakIds);
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
