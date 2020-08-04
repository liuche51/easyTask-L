package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.dto.proto.ResultDto;
import com.github.liuche51.easyTask.netty.client.NettyClient;
import com.github.liuche51.easyTask.cluster.ClusterUtil;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTask.netty.client.NettyMsgService;
import com.github.liuche51.easyTask.util.StringConstant;
import com.github.liuche51.easyTask.util.Util;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
        AnnularQueue.getInstance().getConfig().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                if (follows != null) {
                    follows.forEach(x -> {
                        notifyFollowLeaderPosition(x, tryCount,5);
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
     * @Param waiteSecond 重试等待时间
     */
    public static boolean notifyFollowLeaderPosition(Node follow, int tryCount, int waiteSecond) {
        if (tryCount == 0) return false;
        String error = StringConstant.EMPTY;
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setInterfaceName(NettyInterfaceEnum.SYNC_LEADER_POSITION).setSource(AnnularQueue.getInstance().getConfig().getAddress())
                    .setBody(AnnularQueue.getInstance().getConfig().getAddress());
            Dto.Frame frame = NettyMsgService.sendSyncMsg(follow.getClient(), builder.build());
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (StringConstant.TRUE.equals(result.getResult())) {
                return true;
            } else
                error = result.getMsg();
        } catch (Exception e) {
            log.error("notifyFollowLeaderPosition.tryCount=" + tryCount, e);
        } finally {
            tryCount--;
        }
        log.info("notifyFollowLeaderPosition()-> error" + error + ",tryCount=" + tryCount + ",objectHost=" + follow.getAddress());
        try {
            Thread.sleep(waiteSecond*1000);
        } catch (InterruptedException e) {
            log.error("",e);
        }
        return notifyFollowLeaderPosition(follow, tryCount,waiteSecond);
    }

    /**
     * 同步任务数据到follow，批量方式
     * 用于将数据同步给新follow
     * 暂时不支持失败进入选新follow流程。代码注释掉
     * 目前仅在leader心跳follow是否存活那边进行选新follow流程
     *
     * @param schedules
     * @param follow
     * @return
     * @throws InterruptedException
     */
    public static boolean syncDataToFollowBatch(List<Schedule> schedules, Node follow) throws Exception {
        ScheduleDto.ScheduleList.Builder builder0 = ScheduleDto.ScheduleList.newBuilder();
        for (Schedule schedule : schedules) {
            ScheduleDto.Schedule s = schedule.toScheduleDto();
            builder0.addSchedules(s);
        }
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.LEADER_SYNC_DATA_TO_NEW_FOLLOW).setSource(AnnularQueue.getInstance().getConfig().getAddress())
                .setBodyBytes(builder0.build().toByteString());
        NettyClient client = follow.getClientWithCount(AnnularQueue.getInstance().getConfig().getTryCount());
       /* if (client == null) {
            log.info("client == null,so start to syncDataToFollowBatch.");
            Node newFollow = VoteFollows.selectNewFollow(follow,null);
            return syncDataToFollowBatch(schedules, newFollow);
        }*/
        boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), AnnularQueue.getInstance().getConfig().getTryCount());
      /*  if (!ret) {
            log.info("sendSyncMsgWithCount return false,so start to syncDataToFollowBatch.");
            Node newFollow = VoteFollows.selectNewFollow(follow,null);
            return syncDataToFollowBatch(schedules, newFollow);
        }*/
        return ret;
    }
}
