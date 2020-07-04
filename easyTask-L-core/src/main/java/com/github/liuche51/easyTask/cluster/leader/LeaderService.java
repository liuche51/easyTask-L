package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.task.*;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dao.ScheduleSyncDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleSync;
import com.github.liuche51.easyTask.enume.ScheduleSyncStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

/**
 * Leader服务入口
 */
public class LeaderService {
    private static final Logger log = LoggerFactory.getLogger(LeaderService.class);

    /**
     * 节点启动初始化选举follows。
     *
     * @return
     */
    public static void initSelectFollows() throws Exception {
        VoteFollows.initSelectFollows();
    }


    /**
     * 同步删除任务至follow。
     * 先将同步记录表中的状态更新为删除中，删除成功后再更新状态为已删除
     * 后期需要考虑数据一致性的事务机制
     *
     * @param taskId
     * @return
     */
    public static void deleteTaskToFollows(String taskId) throws Exception {
        List<Node> follows = ClusterService.CURRENTNODE.getFollows();
        if (follows != null) {
            Iterator<Node> items = follows.iterator();//防止remove操作导致线程不安全异常
            while (items.hasNext()) {
                Node node = items.next();
                ScheduleSyncDao.updateStatusByScheduleIdAndFollow(taskId, node.getAddress(), ScheduleSyncStatusEnum.DELETEING);
                LeaderUtil.deleteTaskToFollow(taskId, node);
                ScheduleSyncDao.updateStatusByScheduleIdAndFollow(taskId, node.getAddress(), ScheduleSyncStatusEnum.DELETED);
            }
        }
    }

    /**
     * 将失效的leader的备份任务数据删除掉
     * @param oldLeaderAddress
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void deleteOldLeaderBackTask(String oldLeaderAddress) throws SQLException, ClassNotFoundException {
        ScheduleBakDao.deleteBySource(oldLeaderAddress);
    }

    /**
     * 节点对zk的心跳。2s一次
     */
    public static TimerTask initHeartBeatToZK() {
        HeartbeatsTask task=new HeartbeatsTask();
        task.start();
       return task;
    }

    /**
     * 节点对zk的心跳。检查follows是否失效。
     * 失效则进入选举
     */
    public static TimerTask initCheckFollowAlive() {
        CheckFollowsAliveTask task=new CheckFollowsAliveTask();
        task.start();
        return task;
    }

    /**
     * leader同步数据到新follow
     * 目前设计为只有一个线程同步给某个follow
     *
     * @param oldFollow
     * @param newFollow
     */
    public static OnceTask syncDataToNewFollow(Node oldFollow, Node newFollow) {
        SyncDataToNewFollowTask task=new SyncDataToNewFollowTask(oldFollow,newFollow);
        task.start();
        ClusterService.onceTasks.add(task);
        return task;
    }

    /**
     * 新leader将旧leader的备份数据同步给自己的follow
     * 后期需要考虑数据一致性
     *
     * @param oldLeaderAddress
     */
    public static OnceTask submitNewTaskByOldLeader(String oldLeaderAddress) {
        NewLeaderSyncBakDataTask task=new NewLeaderSyncBakDataTask(oldLeaderAddress);
        task.start();
        ClusterService.onceTasks.add(task);
        return task;
    }

}
