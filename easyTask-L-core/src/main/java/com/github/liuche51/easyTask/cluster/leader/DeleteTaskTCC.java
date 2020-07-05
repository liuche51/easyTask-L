package com.github.liuche51.easyTask.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.cluster.ClusterUtil;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.core.Util;
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

public class DeleteTaskTCC {
    /**
     * 删除任务事务第一阶段。
     * 先记入事务表，等到第二阶段提交确认
     * @param taskId  任务ID
     * @param follows
     * @throws Exception
     */
    public static void tryDel(String transactionId,String taskId, List<Node> follows) throws Exception {
        List<String> cancelHost=follows.stream().map(Node::getAddress).collect(Collectors.toList());
        Transaction transaction = new Transaction();
        transaction.setId(transactionId);
        transaction.setContent(taskId);
        transaction.setTable(TransactionTableEnum.SCHEDULE);
        transaction.setStatus(TransactionStatusEnum.TRIED);
        transaction.setType(TransactionTypeEnum.DELETE);
        transaction.setCancelHost(JSONObject.toJSONString(cancelHost));
        TransactionDao.save(transaction);
        //需要将同步记录表原来的提交已同步记录修改为删除中，并更新其事务ID
        ScheduleSyncDao.updateStatusAndTransactionIdByScheduleId(taskId,ScheduleSyncStatusEnum.DELETEING,transaction.getId());
        Iterator<Node> items = follows.iterator();
        while (items.hasNext()) {
            Node follow = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.TRAN_TRYDELTASK).setSource(EasyTaskConfig.getInstance().getzKServerName())
                    .setBody(transaction.getId()+","+taskId);
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), 1);
            if(!ret){
                throw new Exception("tryDel->sendSyncMsgWithCount():exception! ");
            }
        }
    }

    /**
     * 确认删除任务。第二阶段
     * 先让follows标记事务确认，最后在标记自己的事务确认。这样有利于事务重试（确认删除阶段重试）
     * @param transactionId
     * @param follows
     * @throws Exception
     */
    public static void confirm(String transactionId, List<Node> follows) throws Exception {
        Iterator<Node> items = follows.iterator();
        while (items.hasNext()) {
            Node follow = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.TRAN_CONFIRMDELTASK).setSource(EasyTaskConfig.getInstance().getzKServerName())
                    .setBody(transactionId);
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), 1);
            if (ret) {
                //follow已经标记为确认删除了，就可以认为是已经完成删除
                ScheduleSyncDao.updateStatusByTransactionIdAndFollow(transactionId, follow.getAddress(), ScheduleSyncStatusEnum.DELETED);
            } else
                throw new Exception("confirm()->sendSyncMsgWithCount() exception！");
        }
        //follow都标记为确认删除了。就可以认为数据已经删除，后面不需要重试标记确认删除了。否则后面还需要重试本方法确认删除
        TransactionDao.updateStatusById(transactionId,TransactionStatusEnum.CONFIRM);
    }

    /**
     * 事务回滚阶段。
     * 暂时不需要实现。删除任务回滚没有意义。使用最大努力通知方式，确认标记删除
     * @param transactionId
     * @param follows
     * @throws Exception
     */
    public static void cancel(String transactionId,List<Node> follows) throws Exception {

    }
}
