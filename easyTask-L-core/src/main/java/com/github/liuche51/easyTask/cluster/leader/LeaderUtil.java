package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.netty.client.NettyClient;
import com.github.liuche51.easyTask.cluster.ClusterUtil;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.util.NettyInterfaceEnum;
import com.github.liuche51.easyTask.util.StringConstant;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

/**
 * leader类
 */
public class LeaderUtil {
    private static final Logger log = LoggerFactory.getLogger(LeaderUtil.class);

    /**
     * 通知follows当前Leader位置。异步调用即可
     *
     * @return
     */
    public static boolean notifyFollowsLeaderPosition(List<Node> follows, int tryCount) {
        EasyTaskConfig.getInstance().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                if (follows != null) {
                    follows.forEach(x -> {
                        notifyFollowLeaderPosition(x, tryCount);
                    });
                }
            }
        });
        return true;
    }

    /**
     * @param follow
     * @param tryCount
     * @return
     */
    public static boolean notifyFollowLeaderPosition(Node follow, int tryCount) {
        if (tryCount == 0) return false;
        final boolean[] ret = {false};
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setInterfaceName(NettyInterfaceEnum.SYNC_LEADER_POSITION).setSource(EasyTaskConfig.getInstance().getzKServerName())
                    .setBody(EasyTaskConfig.getInstance().getzKServerName());
            ChannelFuture future = follow.getClient().sendASyncMsgWithoutPromise(builder.build());
            tryCount--;
            future.addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    if (future.isSuccess()) {
                        ret[0] = true;
                    }
                }
            });
            if (ret[0])
                return true;
        } catch (Exception e) {
            tryCount--;
            log.error("notifyFollowLeaderPosition.tryCount=" + tryCount, e);
        }
        return notifyFollowLeaderPosition(follow, tryCount);
    }
    /**
     * 同步任务数据到follow
     *暂时不支持失败进入选新follow流程。代码注释掉
     * 目前仅在leader心跳follow是否存活那边进行选新follow流程
     * @param schedule
     * @param follow
     * @return
     * @throws InterruptedException
     */
    public static boolean syncDataToFollow(Schedule schedule, Node follow) throws Exception {
        ScheduleDto.Schedule s = schedule.toScheduleDto();
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(s.getId()).setInterfaceName(NettyInterfaceEnum.SYNC_SCHEDULE_BACKUP).setSource(EasyTaskConfig.getInstance().getzKServerName())
                .setBodyBytes(s.toByteString());
        NettyClient client = follow.getClientWithCount(EasyTaskConfig.getInstance().getTryCount());
       /* if (client == null) {
            log.info("client == null,so start to selectNewFollow.");
            Node newFollow = VoteFollows.selectNewFollow(follow,null);
            return syncDataToFollow(schedule, newFollow);
        }*/
        boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), EasyTaskConfig.getInstance().getTryCount());
       /* if (!ret) {
            log.info("sendSyncMsgWithCount return false,so start to selectNewFollow.");
            Node newFollow = VoteFollows.selectNewFollow(follow,null);
            return syncDataToFollow(schedule, newFollow);
        }*/
        return true;
    }
    /**
     * 同步任务数据到follow，批量方式
     *用于将数据同步给新follow
     *暂时不支持失败进入选新follow流程。代码注释掉
     * 目前仅在leader心跳follow是否存活那边进行选新follow流程
     * @param schedules
     * @param follow
     * @return
     * @throws InterruptedException
     */
    public static boolean syncDataToFollowBatch(List<Schedule> schedules, Node follow) throws Exception {
        ScheduleDto.ScheduleList.Builder builder0=ScheduleDto.ScheduleList.newBuilder();
        for(Schedule schedule:schedules){
            ScheduleDto.Schedule s = schedule.toScheduleDto();
            builder0.addSchedules(s);
        }
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(UUID.randomUUID().toString()).setInterfaceName(NettyInterfaceEnum.SYNC_SCHEDULE_BACKUP_BATCH).setSource(EasyTaskConfig.getInstance().getzKServerName())
                .setBodyBytes(builder0.build().toByteString());
        NettyClient client = follow.getClientWithCount(EasyTaskConfig.getInstance().getTryCount());
       /* if (client == null) {
            log.info("client == null,so start to syncDataToFollowBatch.");
            Node newFollow = VoteFollows.selectNewFollow(follow,null);
            return syncDataToFollowBatch(schedules, newFollow);
        }*/
        boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), EasyTaskConfig.getInstance().getTryCount());
      /*  if (!ret) {
            log.info("sendSyncMsgWithCount return false,so start to syncDataToFollowBatch.");
            Node newFollow = VoteFollows.selectNewFollow(follow,null);
            return syncDataToFollowBatch(schedules, newFollow);
        }*/
        return true;
    }

    /**
     * 同步删除任务至follow。
     *暂时不支持失败进入选新follow流程。代码注释掉
     * 目前仅在leader心跳follow是否存活那边进行选新follow流程
     * @param taskId
     * @return
     */
    public static boolean deleteTaskToFollow(String taskId, Node follow) throws Exception {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(taskId).setInterfaceName(NettyInterfaceEnum.DELETE_SCHEDULEBACKUP).setSource(EasyTaskConfig.getInstance().getzKServerName())
                .setBody(taskId);
        NettyClient client = follow.getClientWithCount(EasyTaskConfig.getInstance().getTryCount());
      /*  if (client == null) {
            log.info("client == null,so start to selectNewFollow.");
            Node newFollow = VoteFollows.selectNewFollow(follow,null);
            return deleteTaskToFollow(taskId, newFollow);
        }*/
        boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), EasyTaskConfig.getInstance().getTryCount());
      /*  if (!ret) {
            log.info("sendSyncMsgWithCount return false,so start to selectNewFollow.");
            Node newFollow = VoteFollows.selectNewFollow(follow,null);
            return deleteTaskToFollow(taskId, newFollow);
        }*/
        return true;
    }
}
