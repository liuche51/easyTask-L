package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.task.CheckFollowsAliveTask;
import com.github.liuche51.easyTask.cluster.task.HeartbeatsTask;
import com.github.liuche51.easyTask.cluster.task.TimerTask;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dao.ScheduleSyncDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.ScheduleSync;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTask.enume.ScheduleSyncStatusEnum;
import com.github.liuche51.easyTask.util.exception.VotedException;
import com.github.liuche51.easyTask.util.exception.VotingException;
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
     * 同步任务至follow。
     * 先写同步记录，同步成功再更新同步状态为成功。如果发生异常，则认为本次任务提交失败
     * 后期需要考虑数据一致性的事务机制
     *
     * @param schedule
     * @return
     */
    public static void syncDataToFollows(Schedule schedule) throws Exception {
        List<Node> follows = ClusterService.CURRENTNODE.getFollows();
        if (follows != null) {
            Iterator<Node> items = follows.iterator();//防止remove操作导致线程不安全异常
            while (items.hasNext()) {
                Node follow = items.next();
                ScheduleSync scheduleSync = new ScheduleSync();
                scheduleSync.setScheduleId(schedule.getId());
                scheduleSync.setFollow(follow.getAddress());
                scheduleSync.setStatus(ScheduleSyncStatusEnum.SYNCING);
                ScheduleSyncDao.save(scheduleSync);
                LeaderUtil.syncDataToFollow(schedule, follow);
                ScheduleSyncDao.updateStatusByScheduleIdAndFollow(schedule.getId(), follow.getAddress(), ScheduleSyncStatusEnum.SYNCED);
            }
        }
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
    public static void syncDataToNewFollow(Node oldFollow, Node newFollow) {
        Thread th1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    //先将失效的follow数据同步标记为未同步，同时修改其follow标识
                    ScheduleSyncDao.updateFollowAndStatusByFollow(oldFollow.getAddress(), newFollow.getAddress(), ScheduleSyncStatusEnum.UNSYNC);
                    while (true) {
                        //获取批次数据
                        List<ScheduleSync> list = ScheduleSyncDao.selectByFollowAndStatusWithCount(newFollow.getAddress(), ScheduleSyncStatusEnum.UNSYNC, 5);
                        if (list.size() == 0) {//如果已经同步完，标记状态并则跳出循环
                            newFollow.setDataStatus(NodeSyncDataStatusEnum.SYNC);
                            break;
                        }
                        String[] ids = list.stream().distinct().map(ScheduleSync::getScheduleId).toArray(String[]::new);
                        ScheduleSyncDao.updateStatusByFollowAndScheduleIds(newFollow.getAddress(), ids, ScheduleSyncStatusEnum.SYNCING);
                        List<Schedule> list1 = ScheduleDao.selectByIds(ids);
                        boolean ret = LeaderUtil.syncDataToFollowBatch(list1, oldFollow);
                        if (ret)
                            ScheduleSyncDao.updateStatusByFollowAndStatus(newFollow.getAddress(), ScheduleSyncStatusEnum.SYNCING, ScheduleSyncStatusEnum.SYNCED);
                    }
                } catch (VotingException e) {
                    //同步数据异常，进入选举新follow。但此时刚好有其他地方触发正在选举中。当前新follow可能又失效了。
                    //此时就没必要继续同步数据给当前新follow了。终止同步线程
                    log.info("normally exception error.can ignore.{}", e.getMessage());
                } catch (VotedException e) {
                    //原因同上VotingException
                    log.info("normally exception error.can ignore.{}", e.getMessage());
                } catch (Exception e) {
                    log.error("syncDataToNewFollow()", e);
                }
            }
        });
        th1.start();
    }

    /**
     * 新leader将旧leader的备份数据同步给自己的follow
     * 后期需要考虑数据一致性
     *
     * @param oldLeaderAddress
     */
    public static void submitNewTaskByOldLeader(String oldLeaderAddress) {
        Thread th1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    List<ScheduleBak> baks = ScheduleBakDao.getBySourceWithCount(oldLeaderAddress, 5);
                    baks.forEach(x -> {
                        Task task = Task.valueOf(x);
                        try {
                            AnnularQueue.getInstance().submitForInner(task);//模拟客户端重新提交任务
                            ScheduleBakDao.delete(x.getId());
                        } catch (Exception e) {
                            log.error("submitNewTaskByOldLeader()->", e);
                        }
                    });
                } catch (Exception e) {
                    log.error("submitNewTaskByOldLeader()->", e);
                }
            }
        });
        th1.start();
    }
}
