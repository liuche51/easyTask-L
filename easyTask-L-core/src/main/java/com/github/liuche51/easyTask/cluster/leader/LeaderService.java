package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.dto.Schedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Leader服务入口
 */
public class LeaderService {
    private static final Logger log = LoggerFactory.getLogger(LeaderService.class);
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
    /**
     * 节点对zk的心跳。2s一次
     */
    public static void heartBeatToZK() {
        LeaderHeartbeat.heartBeatToZK();
    }
    /**
     * 节点对zk的心跳。检查follows是否失效。
     * 失效则进入选举
     */
    public static void heartBeatToFollow() {
        LeaderHeartbeat.heartBeatToFollow();
    }
}
