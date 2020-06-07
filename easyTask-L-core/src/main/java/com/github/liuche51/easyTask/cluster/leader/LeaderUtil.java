package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.ClusterUtil;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.register.ZKService;
import com.github.liuche51.easyTask.util.DateUtils;
import com.github.liuche51.easyTask.util.StringConstant;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.log4j.Logger;

import java.time.ZonedDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * leader类
 */
public class LeaderUtil {
    private static final Logger log = Logger.getLogger(LeaderUtil.class);

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
            builder.setInterfaceName(StringConstant.SYNC_LEADER_POSITION).setSource(EasyTaskConfig.getInstance().getzKServerName())
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
     * 从zk获取可用的follow，并排除自己
     *
     * @return
     */
    public static List<String> getAvailableFollows() throws InterruptedException {
        int count = EasyTaskConfig.getInstance().getBackupCount();
        List<String> availableFollows = ZKService.getChildrenByNameSpase();
        //排除自己
        Optional<String> temp = availableFollows.stream().filter(x -> x.equals(EasyTaskConfig.getInstance().getzKServerName())).findFirst();
        if (temp.isPresent())
            availableFollows.remove(temp.get());
        ClusterService.CURRENTNODE.getFollows().forEach(x->{//排除现有的
            Optional<String> temp1 = availableFollows.stream().filter(y -> y.equals(x.getHost()+":"+x.getHost())).findFirst();
            if (temp1.isPresent())
                availableFollows.remove(temp1.get());
        });
        if (availableFollows.size() < count)//如果可选备库节点数量不足，则等待1s，然后重新选。注意：等待会阻塞整个服务可用性
        {
            log.info("availableFollows is not enough! only has " + availableFollows.size());
            Thread.sleep(1000);
            return getAvailableFollows();
        } else
            return availableFollows;
    }

    /**
     * 从可用follows中选择若干个follow
     *
     * @param count            需要的数量
     * @param availableFollows 可用follows
     */
    public static List<Node> selectFollows(int count, List<String> availableFollows) {
        List<Node> follows = new LinkedList<>();//备选follows
        int size=availableFollows.size();
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            int index = random.nextInt(availableFollows.size());//随机生成的随机数范围就变成[0,size)。
            ZKNode node2 = ZKService.getDataByPath(StringConstant.CHAR_SPRIT + availableFollows.get(index));
            //如果最后心跳时间超过60s，则直接删除该节点信息。
            if (ZonedDateTime.now().minusSeconds(EasyTaskConfig.getInstance().getDeleteZKTimeOunt())
                    .compareTo(DateUtils.parse(node2.getLastHeartbeat())) > 0) {
                ZKService.deleteNodeByPathIgnoreResult(StringConstant.CHAR_SPRIT + availableFollows.get(index));
            } else if (ZonedDateTime.now().minusSeconds(EasyTaskConfig.getInstance().getSelectLeaderZKNodeTimeOunt())
                    .compareTo(DateUtils.parse(node2.getLastHeartbeat())) > 0) {
                //如果最后心跳时间超过30s，也不能将该节点作为follow
            } else if (follows.size() < count) {
                follows.add(new Node(node2.getHost(), node2.getPort()));
                if (follows.size() == count)//已选数量够了就跳出
                    break;
            }
            availableFollows.remove(index);
        }
        return follows;
    }

    /**
     * 选择新follow
     * leader同步数据失败或心跳检测失败，则进入选新follow程序
     *
     * @return
     */
    public static Node selectNewFollow(Node oldFollow) throws InterruptedException {
        if (ClusterService.CURRENTNODE.getFollows().contains(oldFollow))
            ClusterService.CURRENTNODE.getFollows().remove(oldFollow);//移除失效的follow
        List<String> availableFollows = LeaderUtil.getAvailableFollows();
        List<Node> follows = LeaderUtil.selectFollows(1, availableFollows);
        if (follows.size() < 1) selectNewFollow(oldFollow);//数量不够递归重新选
        ClusterService.CURRENTNODE.getFollows().add(follows.get(0));
        //通知follows当前Leader位置
        LeaderUtil.notifyFollowsLeaderPosition(follows, EasyTaskConfig.getInstance().getTryCount());
        return follows.get(0);
    }

    /**
     * 同步任务数据到follow
     *
     * @param schedule
     * @param follow
     * @return
     * @throws InterruptedException
     */
    public static boolean syncDataToFollow(Schedule schedule, Node follow) throws InterruptedException {
        ScheduleDto.Schedule s = schedule.toScheduleDto();
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(s.getId()).setInterfaceName(StringConstant.SYNC_SCHEDULE_BACKUP).setSource(EasyTaskConfig.getInstance().getzKServerName())
                .setBodyBytes(s.toByteString());
        NettyClient client = follow.getClientWithCount(EasyTaskConfig.getInstance().getTryCount());
        if (client == null) {
            log.info("client == null,so start to selectNewFollow.");
            Node newFollow = selectNewFollow(follow);
            return syncDataToFollow(schedule, newFollow);
        }
        boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), EasyTaskConfig.getInstance().getTryCount());
        if (!ret) {
            log.info("sendSyncMsgWithCount return false,so start to selectNewFollow.");
            Node newFollow = selectNewFollow(follow);
            return syncDataToFollow(schedule, newFollow);
        }
        return true;
    }

    /**
     * 同步删除任务至follow。
     *
     * @param taskId
     * @return
     */
    public static boolean deleteTaskToFollow(String taskId, Node follow) throws InterruptedException {
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        builder.setIdentity(taskId).setInterfaceName(StringConstant.DELETE_SCHEDULEBACKUP).setSource(EasyTaskConfig.getInstance().getzKServerName())
                .setBody(taskId);
        NettyClient client = follow.getClientWithCount(EasyTaskConfig.getInstance().getTryCount());
        if (client == null) {
            log.info("client == null,so start to selectNewFollow.");
            Node newFollow = selectNewFollow(follow);
            return deleteTaskToFollow(taskId, newFollow);
        }
        boolean ret = ClusterUtil.sendSyncMsgWithCount(client, builder.build(), EasyTaskConfig.getInstance().getTryCount());
        if (!ret) {
            log.info("sendSyncMsgWithCount return false,so start to selectNewFollow.");
            Node newFollow = selectNewFollow(follow);
            return deleteTaskToFollow(taskId, newFollow);
        }
        return true;
    }
}
