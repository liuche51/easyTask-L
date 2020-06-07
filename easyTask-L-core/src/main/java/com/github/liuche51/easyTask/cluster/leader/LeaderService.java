package com.github.liuche51.easyTask.cluster.leader;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.backup.server.NettyServer;
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
 * Leader服务入口
 */
public class LeaderService {
    private static final Logger log = Logger.getLogger(LeaderService.class);

    /**
     * 节点启动初始化选举follows。
     *
     * @return
     */
    public static void initSelectFollows() throws InterruptedException {
        int count = EasyTaskConfig.getInstance().getBackupCount();
        List<String> availableFollows = LeaderUtil.getAvailableFollows();
        List<Node> follows = LeaderUtil.selectFollows(count, availableFollows);
        if (follows.size() < count) {
            log.info("follows.size() < count,so start to initSelectFollows");
            initSelectFollows();//数量不够递归重新选
        }
        ClusterService.CURRENTNODE.setFollows(follows);
        //通知follows当前Leader位置
        LeaderUtil.notifyFollowsLeaderPosition(follows, EasyTaskConfig.getInstance().getTryCount());
    }

    /**
     * 同步任务至follow。
     *
     * @param schedule
     * @return
     */
    public static void syncDataToFollows(Schedule schedule) throws InterruptedException {
        List<Node> follows = ClusterService.CURRENTNODE.getFollows();
        if (follows != null) {
            for (Node follow : follows) {
                LeaderUtil.syncDataToFollow(schedule, follow);
            }
        }
    }


    /**
     * 同步删除任务至follow。
     *
     * @param taskId
     * @return
     */
    public static void deleteTaskToFollows(String taskId) throws InterruptedException {
        List<Node> follows = ClusterService.CURRENTNODE.getFollows();
        if (follows != null) {
            for (Node follow : follows) {
                LeaderUtil.deleteTaskToFollow(taskId, follow);
            }
        }
    }
}
