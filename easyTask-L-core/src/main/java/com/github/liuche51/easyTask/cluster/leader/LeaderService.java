package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dao.ScheduleSyncDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleSync;
import com.github.liuche51.easyTask.util.ScheduleSyncStatusEnum;
import com.github.liuche51.easyTask.util.exception.VotedException;
import com.github.liuche51.easyTask.util.exception.VotingException;
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
     *
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
            Iterator<Node> items = follows.iterator();//防止remove操作导致线程不安全异常
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
            Iterator<Node> items = follows.iterator();//防止remove操作导致线程不安全异常
            while (items.hasNext()) {
                LeaderUtil.deleteTaskToFollow(taskId, items.next());
            }
        }
    }

    /**
     * 节点对zk的心跳。2s一次
     */
    public static void initHeartBeatToZK() {
        LeaderHeartbeat.heartBeatToZK();
    }

    /**
     * 节点对zk的心跳。检查follows是否失效。
     * 失效则进入选举
     */
    public static void initCheckFollowAlive() {
        LeaderHeartbeat.heartBeatToFollow();
    }

    /**
     * leader同步数据到新follow
     * 目前设计为只有一个线程同步给某个follow
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
                        List<ScheduleSync> list = ScheduleSyncDao.selectByFollowAndStatusWithCount(newFollow.getAddress(), ScheduleSyncStatusEnum.UNSYNC, 5);
                        if (list.size() == 0) break;//如果已经同步完，则跳出循环
                        ScheduleSyncDao.updateStatusByFollowAndStatus(newFollow.getAddress(),ScheduleSyncStatusEnum.UNSYNC,ScheduleSyncStatusEnum.SYNCING);
                        String[] ids = list.stream().distinct().map(ScheduleSync::getScheduleId).toArray(String[]::new);
                        List<Schedule> list1 = ScheduleDao.selectByIds(ids);
                        boolean ret=LeaderUtil.syncDataToFollowBatch(list1, oldFollow);
                        if(ret)
                            ScheduleSyncDao.updateStatusByFollowAndStatus(newFollow.getAddress(),ScheduleSyncStatusEnum.SYNCING,ScheduleSyncStatusEnum.SYNCED);
                    }
                }
                catch (VotingException e){
                    //同步数据异常，进入选举新follow。但此时刚好有其他地方触发正在选举中。当前新follow可能又失效了。
                    //此时就没必要继续同步数据给当前新follow了。终止同步线程
                    log.info("normally exception error.can ignore.{}",e.getMessage());
                }
                catch (VotedException e){
                    //原因同上VotingException
                    log.info("normally exception error.can ignore.{}",e.getMessage());
                }
                catch (Exception e) {
                    log.error("syncDataToNewFollow()", e);
                }
            }
        });
        th1.start();
    }
}
