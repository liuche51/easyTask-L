package com.github.liuche51.easyTask.cluster.follow;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.core.Util;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.dto.zk.ZKHost;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.register.ZKService;
import com.github.liuche51.easyTask.util.DateUtils;
import com.github.liuche51.easyTask.util.StringConstant;
import com.github.liuche51.easyTask.util.StringUtils;
import org.apache.log4j.Logger;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

/**
 * Follow服务入口
 */
public class FollowService {
    private static final Logger log = Logger.getLogger(LeaderService.class);

    /**
     * 接受leader同步任务入备库
     *
     * @param schedule
     */
    public static void saveScheduleBak(ScheduleDto.Schedule schedule) {
        ScheduleBak bak = ScheduleBak.valueOf(schedule);
        ScheduleBakDao.save(bak);
    }

    /**
     * 删除备库任务
     *
     * @param taskId
     */
    public static void deleteScheduleBak(String taskId) {
        ScheduleBakDao.delete(taskId);
    }

    /**
     * 更新leader位置信息
     *
     * @param leader
     * @return
     */
    public static boolean updateLeaderPosition(String leader) {
        try {
            if (StringUtils.isNullOrEmpty(leader)) return false;
            String[] temp = leader.split(":");
            if (temp.length != 2) return false;
            Map<String, Node> leaders = ClusterService.CURRENTNODE.getLeaders();
            leaders.put(leader, new Node(temp[0], Integer.valueOf(temp[1]).intValue()));
            return true;
        } catch (Exception e) {
            log.error("updateLeaderPosition", e);
            return false;
        }
    }

    /**
     * 节点对zk的心跳。2s一次
     */
    public static void heartBeatToLeader() {
        Thread th1 = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Map<String, Node> leaders = ClusterService.CURRENTNODE.getLeaders();
                        for (Map.Entry<String, Node> item : leaders.entrySet()) {
                            String path = StringConstant.CHAR_SPRIT + item.getValue().getAddress();
                            ZKNode node = ZKService.getDataByPath(path);
                            //如果最后心跳时间超过60s，则直接删除该节点信息。
                            if (ZonedDateTime.now().minusSeconds(EasyTaskConfig.getInstance().getDeleteZKTimeOunt())
                                    .compareTo(DateUtils.parse(node.getLastHeartbeat())) > 0) {
                                ZKService.deleteNodeByPathIgnoreResult(path);
                            }
                            //如果最后心跳时间超过30s，进入选举新leader流程
                            else if (ZonedDateTime.now().minusSeconds(EasyTaskConfig.getInstance().getSelectLeaderZKNodeTimeOunt())
                                    .compareTo(DateUtils.parse(node.getLastHeartbeat())) > 0) {
                                List<ZKHost> follows = node.getFollows();
                                ZKHost newLeader = null;
                                int smallest = 0;
                                for (ZKHost x : follows) {
                                    if (smallest == 0||x.getAddress().hashCode() < smallest) {
                                        smallest = x.getAddress().hashCode();
                                        newLeader = x;
                                    }
                                }
                                //自己就是新leader
                                if(newLeader!=null&&newLeader.getAddress().equals(EasyTaskConfig.getInstance().getzKServerName())){
                                    List<ScheduleBak> baks=ScheduleBakDao.getBySource(item.getValue().getAddress());
                                    baks.forEach(x->{
                                        Task task=Task.valueOf(x);
                                        try {
                                            AnnularQueue.getInstance().submit(task);//模拟客户端重新提交任务

                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                    });
                                }

                            }
                        }
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
            }
        });
        th1.start();
    }
}
