package com.github.liuche51.easyTask.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.cluster.ClusterUtil;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dao.ScheduleSyncDao;
import com.github.liuche51.easyTask.dao.TransactionDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleSync;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.dto.Transaction;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.enume.*;
import com.github.liuche51.easyTask.netty.client.NettyClient;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SaveTCC {
    /**
     * 提交任务事务第一阶段。
     * 先记入事务表，等到第二阶段提交确认
     * @param task
     * @param follows
     * @throws Exception
     */
    public static void trySave(Task task, List<Node> follows) throws Exception {
        List<String> cancelHost=follows.stream().map(Node::getAddress).collect(Collectors.toList());
        Schedule schedule = Schedule.valueOf(task);
        Transaction transaction = new Transaction();
        transaction.setId(schedule.getId());
        transaction.setContent(JSONObject.toJSONString(schedule));
        transaction.setTable(TransactionTableEnum.SCHEDULE);
        transaction.setStatus(TransactionStatusEnum.TRIED);
        transaction.setType(TransactionTypeEnum.SAVE);
        transaction.setCancelHost(JSONObject.toJSONString(cancelHost));
        TransactionDao.save(transaction);
        Iterator<Node> items = follows.iterator();
        while (items.hasNext()) {
            Node follow = items.next();
            ScheduleSync scheduleSync = new ScheduleSync();
            scheduleSync.setTransactionId(transaction.getId());
            scheduleSync.setScheduleId(schedule.getId());
            scheduleSync.setFollow(follow.getAddress());
            scheduleSync.setStatus(ScheduleSyncStatusEnum.SYNCING);
            ScheduleSyncDao.save(scheduleSync);//记录同步状态表
            ScheduleDto.Schedule s = schedule.toScheduleDto();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(s.getId()).setInterfaceName(NettyInterfaceEnum.TRAN_TRYSAVETASK).setSource(EasyTaskConfig.getInstance().getzKServerName())
                    .setBodyBytes(s.toByteString());
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), 1);
            if(!ret){
                throw new Exception("sendSyncMsgWithCount()->exception! ");
            }
        }

    }

    /**
     * 确认提交任务。第二阶段
     * @param transactionId
     * @param scheduleId
     * @param follows
     * @throws Exception
     */
    public static void confirm(String transactionId, String scheduleId, List<Node> follows) throws Exception {
        Iterator<Node> items = follows.iterator();
        while (items.hasNext()) {
            Node follow = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(transactionId).setInterfaceName(NettyInterfaceEnum.TRAN_CONFIRMSAVETASK).setSource(EasyTaskConfig.getInstance().getzKServerName())
                    .setBody(transactionId);
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), 1);
            if (ret) {
                ScheduleSyncDao.updateStatusByScheduleIdAndFollow(scheduleId, follow.getAddress(), ScheduleSyncStatusEnum.SYNCED);
            } else
                throw new Exception("sendSyncMsgWithCount() exception！");
        }
        TransactionDao.updateStatusById(transactionId,TransactionStatusEnum.CONFIRM);
    }

    /**
     * 事务回滚阶段。
     * @param transactionId
     * @param follows
     * @throws Exception
     */
    public static void cancel(String transactionId,List<Node> follows) throws Exception {
        TransactionDao.updateStatusById(transactionId,TransactionStatusEnum.CANCEL);//自己优先标记需回滚
        retryCancel( transactionId, follows);
    }
    public static void retryCancel(String transactionId, List<Node> follows) throws Exception {
        Iterator<Node> items = follows.iterator();
        while (items.hasNext()) {
            Node follow = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(transactionId).setInterfaceName(NettyInterfaceEnum.TRAN_CANCELSAVETASK).setSource(EasyTaskConfig.getInstance().getzKServerName())
                    .setBody(transactionId);
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), 1);
            if (ret) {
                ScheduleSyncDao.deleteByTransactionIdAndFollow(transactionId, follow.getAddress());
            } else
                throw new Exception("sendSyncMsgWithCount() exception！");
        }
    }
}
