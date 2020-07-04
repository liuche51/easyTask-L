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
import com.github.liuche51.easyTask.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTask.enume.ScheduleSyncStatusEnum;
import com.github.liuche51.easyTask.enume.TransactionStatusEnum;
import com.github.liuche51.easyTask.enume.TransactionTypeEnum;
import com.github.liuche51.easyTask.netty.client.NettyClient;
import java.util.Iterator;
import java.util.List;

public class SaveTCC {
    public static void trySave(Task task, List<Node> follows) throws Exception {
        Schedule schedule = Schedule.valueOf(task);
        Transaction transaction = new Transaction();
        transaction.setId(schedule.getId());
        transaction.setContent(JSONObject.toJSONString(schedule));
        transaction.setStatus(TransactionStatusEnum.STARTED);
        transaction.setType(TransactionTypeEnum.SAVE);
        TransactionDao.save(transaction);
        Iterator<Node> items = follows.iterator();
        while (items.hasNext()) {
            Node follow = items.next();
            ScheduleSync scheduleSync = new ScheduleSync();
            scheduleSync.setScheduleId(schedule.getId());
            scheduleSync.setFollow(follow.getAddress());
            scheduleSync.setStatus(ScheduleSyncStatusEnum.SYNCING);
            ScheduleSyncDao.save(scheduleSync);
            LeaderUtil.syncPreDataToFollow(schedule, follow);
        }

    }

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
    }

    public static void cancel(String transactionId, String scheduleId, List<Node> follows) throws Exception {
        Iterator<Node> items = follows.iterator();
        while (items.hasNext()) {
            Node follow = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(transactionId).setInterfaceName(NettyInterfaceEnum.TRAN_CANCELSAVETASK).setSource(EasyTaskConfig.getInstance().getzKServerName())
                    .setBody(transactionId);
            NettyClient client = follow.getClientWithCount(1);
            boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), 1);
            if (ret) {
                ScheduleSyncDao.deleteByScheduleIdAndFollow(scheduleId, follow.getAddress());
            } else
                throw new Exception("sendSyncMsgWithCount() exception！");
        }
    }
}
