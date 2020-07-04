package com.github.liuche51.easyTask.cluster;

import com.github.liuche51.easyTask.cluster.follow.FollowService;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.cluster.leader.SaveTCC;
import com.github.liuche51.easyTask.cluster.task.*;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.core.Util;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dao.ScheduleSyncDao;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.zk.ZKService;
import com.github.liuche51.easyTask.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class ClusterService {
    private static Logger log = LoggerFactory.getLogger(ClusterService.class);
    /**
     * 当前集群节点的Node对象
     */
    public static Node CURRENTNODE;
    /**
     * 集群一次性任务线程集合。
     * 系统没有重启只是初始化了集群initCurrentNode()。此时也需要立即停止运行的一次性后台任务
     * 需要定时检查其中的线程是否已经运行完，完了需要移除线程对象，释放内存资源
     */
    public static List<OnceTask> onceTasks = new LinkedList<OnceTask>();
    /**
     * 集群定时任务线程集合。
     * 系统没有重启只是初始化了集群initCurrentNode()。此时需要停止之前的定时任务，重新启动新的
     */
    public static List<TimerTask> timerTasks = new LinkedList<TimerTask>();

    /**
     * 初始化当前节点的集群。(系统重启或心跳超时重启)
     * zk注册，选follows,开始心跳
     * 这里不考虑短暂宕机重启继续使用原follows情况。让原follows等待超时后重新选举leader就好了
     *
     * @return
     */
    public static boolean initCurrentNode() throws Exception {
        clearThreadTask();
        deleteAllData();
        CURRENTNODE = new Node(Util.getLocalIP(), EasyTaskConfig.getInstance().getServerPort());
        ZKNode node = new ZKNode(CURRENTNODE.getHost(), CURRENTNODE.getPort());
        node.setCreateTime(DateUtils.getCurrentDateTime());
        node.setLastHeartbeat(DateUtils.getCurrentDateTime());
        ZKService.register(node);
        timerTasks.add(LeaderService.initHeartBeatToZK());
        LeaderService.initSelectFollows();
        node.setFollows(Util.nodeToZKHost(CURRENTNODE.getFollows()));
        ZKService.setDataByCurrentNode(node);
        timerTasks.add(LeaderService.initCheckFollowAlive());
        timerTasks.add(FollowService.initCheckLeaderAlive());
        timerTasks.add(clearScheduleBak());
        timerTasks.add(startSubmitTransactionTask());
        return true;
    }

    /**
     * 任务数据持久化。含同步至备份库
     * 后期需要考虑数据一致性的事务机制
     *
     * @throws Exception
     */
    public static void save(Task task) throws Exception {
        //防止多线程下，follow元素操作竞争问题。确保参与提交的follow不受集群选举影响
        List<Node> follows = new ArrayList<>(CURRENTNODE.getFollows().size());
        Iterator<Node> items = CURRENTNODE.getFollows().iterator();
        while (items.hasNext()) {
            follows.add(items.next());
        }
        if (follows.size() != EasyTaskConfig.getInstance().getBackupCount())
            throw new Exception("save() exception！follows.size() not please try again");
        try {
            SaveTCC.trySave(task, follows);
            SaveTCC.confirm(task.getScheduleExt().getId(), task.getScheduleExt().getId(), follows);
        } catch (Exception e) {
            SaveTCC.cancel(task.getScheduleExt().getId(), task.getScheduleExt().getId(), follows);
            throw new Exception("task submit failed!");
        }
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

    public static TimerTask clearScheduleBak() {
        ClearScheduleBakTask task = new ClearScheduleBakTask();
        task.start();
        return task;
    }

    /**
     * 清理掉所有定时或后台线程任务
     */
    public static void clearThreadTask() {
        timerTasks.forEach(x -> {//先停止目前所有内部定时任务线程工作
            x.setExit(true);
        });
        timerTasks.clear();
        onceTasks.forEach(x -> {
            x.setExit(true);
        });
        onceTasks.clear();
    }
    /**
     * 启动批量事务数据提交任务
     * 次次写优化成批量写
     */
    public static TimerTask startSubmitTransactionTask() {
        CommitTransactionTask task=new CommitTransactionTask();
        task.start();
        return task;
    }
}
