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
import java.util.*;

/**
 * Leader服务入口
 */
public class LeaderService {
    private static final Logger log = Logger.getLogger(LeaderService.class);
    /**
     * 节点启动初始化选举follows。
     * @return
     */
    public static void initSelectFollows() throws Exception {
        VoteFollows.initSelectFollows();
    }
    /**
     * 同步任务至follow。
     *
     * @param schedule
     * @return
     */
    public static void syncDataToFollows(Schedule schedule) throws Exception {
        List<Node> follows = ClusterService.CURRENTNODE.getFollows();
        if (follows != null) {
            Iterator<Node> items=follows.iterator();//防止remove操作导致线程不安全异常
            while (items.hasNext()) {
                LeaderUtil.syncDataToFollow(schedule, items.next());
            }
        }
    }

    /**
     * 同步删除任务至follow。
     *
     * @param taskId
     * @return
     */
    public static void deleteTaskToFollows(String taskId) throws Exception {
        List<Node> follows = ClusterService.CURRENTNODE.getFollows();
        if (follows != null) {
            Iterator<Node> items=follows.iterator();//防止remove操作导致线程不安全异常
            while (items.hasNext()){
                LeaderUtil.deleteTaskToFollow(taskId, items.next());
            }
        }
    }
}
