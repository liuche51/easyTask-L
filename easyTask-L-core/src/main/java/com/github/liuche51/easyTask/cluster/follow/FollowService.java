package com.github.liuche51.easyTask.cluster.follow;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Follow服务入口
 */
public class FollowService {
    private static final Logger log = LoggerFactory.getLogger(LeaderService.class);

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
     * 接受leader批量同步任务入备库
     *
     * @param scheduleList
     */
    public static void saveScheduleBakBatch(ScheduleDto.ScheduleList scheduleList) throws Exception {
        List<ScheduleDto.Schedule> list= scheduleList.getSchedulesList();
        if(list==null) return;
        List<ScheduleBak> baklist=new ArrayList<>(list.size());
        list.forEach(x->{
            ScheduleBak bak = ScheduleBak.valueOf(x);
            baklist.add(bak);
        });
        ScheduleBakDao.saveBatch(baklist);
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
     * 节点对zk的心跳。检查leader是否失效。
     * 失效则进入选举
     */
    public static void initCheckLeaderAlive() {
        FollowHeartbeat.heartBeatToLeader();
    }
    public static void submitNewTaskByOldLeader(String oldLeaderAddress){
        List<ScheduleBak> baks= ScheduleBakDao.getBySourceWithCount(oldLeaderAddress,5);
        baks.forEach(x->{
            Task task=Task.valueOf(x);
            try {
                AnnularQueue.getInstance().submit(task);//模拟客户端重新提交任务
                ScheduleBakDao.delete(x.getId());
            } catch (Exception e) {
                log.error("submitNewTaskByOldLeader()->",e);
            }
        });
    }
}
