package com.github.liuche51.easyTask.cluster;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.core.Util;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.register.ZKService;
import com.github.liuche51.easyTask.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;

public class ClusterService {
    private static Logger log = LoggerFactory.getLogger(ClusterService.class);
    /**
     * 当前集群节点的Node对象
     */
    public static Node CURRENTNODE;

    /**
     * 初始化当前节点。
     * zk注册，选follows,开始心跳
     * 这里不考虑短暂宕机重启继续使用原follows情况。让原follows等待超时后重新选举leader就好了
     *
     * @return
     */
    public static boolean initCurrentNode() throws UnknownHostException, InterruptedException ,Exception{
        CURRENTNODE = new Node(Util.getLocalIP(), EasyTaskConfig.getInstance().getServerPort());
        ZKNode node = new ZKNode(CURRENTNODE.getHost(), CURRENTNODE.getPort());
        node.setCreateTime(DateUtils.getCurrentDateTime());
        node.setLastHeartbeat(DateUtils.getCurrentDateTime());
        ZKService.register(node);
        heartBeatToZK();
        LeaderService.initSelectFollows();
        node.setFollows(Util.nodeToZKHost(CURRENTNODE.getFollows()));
        ZKService.setDataByCurrentNode(node);
        return true;
    }
    /**
     * 任务数据持久化。含同步至备份库
     * @throws Exception
     */
    public static void save(Task task) throws Exception {
        Schedule schedule=Schedule.valueOf(task);
        boolean ret1=ScheduleDao.save(schedule);
        if(!ret1) throw new Exception("scheduleDao save exception!");
        //数据高可靠分布式存储
        LeaderService.syncDataToFollows(schedule);
    }

    /**
     * 删除完成的一次性任务。含同步至备份库
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
     * 节点对zk的心跳。2s一次
     */
    public static void heartBeatToZK() {
        Thread th1 = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        ZKNode node = ZKService.getDataByCurrentNode();
                        node.setLastHeartbeat(DateUtils.getCurrentDateTime());
                        node.setLeaders(Util.nodeToZKHost(ClusterService.CURRENTNODE.getLeaders()));//直接将本地数据覆盖到zk
                        node.setFollows(Util.nodeToZKHost(ClusterService.CURRENTNODE.getFollows()));//直接将本地数据覆盖到zk
                        boolean ret = ZKService.setDataByCurrentNode(node);
                        if (!ret) {//设置新值失败，说明zk注册信息已经被follow删除，follows已经重新选举出一个新leader。本节点只能重新选follows了
                            ZKService.register(new ZKNode(CURRENTNODE.getHost(), CURRENTNODE.getPort()));
                            LeaderService.initSelectFollows();
                        }
                        Thread.sleep(EasyTaskConfig.getInstance().getHeartBeat());
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
            }
        });
        th1.start();
    }

}
