package com.github.liuche51.easyTask.cluster;

import com.github.liuche51.easyTask.cluster.follow.FollowService;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.core.Util;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dao.ScheduleSyncDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleSync;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.zk.ZKService;
import com.github.liuche51.easyTask.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class ClusterService {
    private static Logger log = LoggerFactory.getLogger(ClusterService.class);
    /**
     * 当前集群节点的Node对象
     */
    public static Node CURRENTNODE;
    /**
     * 集群定时任务线程集合。
     * 失效相当于重启，需要将原有的线程强制停止。然后重新初始化
     */
    public static List<Thread> threadList=new LinkedList<>();

    /**
     * 初始化当前节点。
     * zk注册，选follows,开始心跳
     * 这里不考虑短暂宕机重启继续使用原follows情况。让原follows等待超时后重新选举leader就好了
     *
     * @return
     */
    public static boolean initCurrentNode() throws Exception {
        threadList.forEach(x->{//先停止目前所有内部定时任务线程工作
            x.interrupt();//似乎没什么作用，经过测试，原线程并没有中断
            x.stop();//暂时使用强制退出，经测试有效
        });
        threadList.clear();
        deleteAllData();
        CURRENTNODE = new Node(Util.getLocalIP(), EasyTaskConfig.getInstance().getServerPort());
        ZKNode node = new ZKNode(CURRENTNODE.getHost(), CURRENTNODE.getPort());
        node.setCreateTime(DateUtils.getCurrentDateTime());
        node.setLastHeartbeat(DateUtils.getCurrentDateTime());
        ZKService.register(node);
        threadList.add(LeaderService.initHeartBeatToZK());
        LeaderService.initSelectFollows();
        node.setFollows(Util.nodeToZKHost(CURRENTNODE.getFollows()));
        ZKService.setDataByCurrentNode(node);
        threadList.add(LeaderService.initCheckFollowAlive());
        threadList.add(FollowService.initCheckLeaderAlive());
        threadList.add(TimerTask.clearScheduleBak());
        return true;
    }

    /**
     * 任务数据持久化。含同步至备份库
     * 后期需要考虑数据一致性的事务机制
     *
     * @throws Exception
     */
    public static void save(Task task) throws Exception {
        Schedule schedule = Schedule.valueOf(task);
        boolean ret1 = ScheduleDao.save(schedule);
        if (!ret1) throw new Exception("scheduleDao save exception!");
        //数据高可靠分布式存储
        LeaderService.syncDataToFollows(schedule);
    }

    /**
     * 删除完成的一次性任务。含同步至备份库
     * 后期需要考虑数据一致性的事务机制
     *
     * @param taskId
     * @return
     */
    public static boolean deleteTask(String taskId) {
        try {
            ScheduleDao.delete(taskId);
            LeaderService.deleteTaskToFollows(taskId);
            return true;
        } catch (Exception e) {
            log.error("deleteTask exception!", e);
        }
        return false;
    }

    /**
     * 清空所有表的记录
     * 节点宕机后，重启。或失去联系zk后又重新连接了。都视为新节点加入集群。加入前需要清空所有记录，避免有重复数据在集群中
     */
    public static void deleteAllData() {
        try {
            ScheduleDao.deleteAll();
            ScheduleBakDao.deleteAll();
            ScheduleSyncDao.deleteAll();
        } catch (Exception e) {
            log.error("deleteAllData exception!", e);
        }
    }

}
