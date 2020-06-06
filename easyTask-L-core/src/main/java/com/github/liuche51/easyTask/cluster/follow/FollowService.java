package com.github.liuche51.easyTask.cluster.follow;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.util.StringUtils;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Follow服务入口
 */
public class FollowService {
    private static final Logger log = Logger.getLogger(LeaderService.class);
    /**
     * 接受leader同步任务入备库
     * @param schedule
     */
    public static void saveScheduleBak(ScheduleDto.Schedule schedule){
        ScheduleBak bak=ScheduleBak.valueOf(schedule);
        ScheduleBakDao.save(bak);
    }
    /**
     * 删除备库任务
     * @param taskId
     */
    public static void deleteScheduleBak(String taskId){
        ScheduleBakDao.delete(taskId);
    }

    /**
     * 更新leader位置信息
     * @param leader
     * @return
     */
    public static boolean updateLeaderPosition(String leader){
        try {
            if(StringUtils.isNullOrEmpty(leader)) return false;
            String[] temp=leader.split(":");
            if(temp.length!=2) return false;
            Map<String,Node> leaders=ClusterService.CURRENTNODE.getLeaders();
            leaders.put(leader,new Node(temp[0],Integer.valueOf(temp[1]).intValue()));
            return true;
        }catch (Exception e){
            log.error("updateLeaderPosition",e);
            return false;
        }
    }
}
