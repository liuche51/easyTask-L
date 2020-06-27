package com.github.liuche51.easyTask.cluster.follow;

import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.dto.zk.ZKHost;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import org.apache.zookeeper.server.quorum.Leader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class VoteLeader {
    private static final Logger log = LoggerFactory.getLogger(VoteLeader.class);
    /**
     * 选举失效leader后新leader
     * 如果新leader是自己，则将备份数据重新提交给自己，否则不用管
     * 选举规则是，所有follows中hashcode最小的
     * @param node
     * @param oldLeaderAddress
     */
    public static void selectNewLeader(ZKNode node,String oldLeaderAddress){
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
            log.info("selectNewLeader():start to submit new task to leader by simulate client");
            LeaderService.submitNewTaskByOldLeader(oldLeaderAddress);
        }else {
            try {
                LeaderService.deleteOldLeaderBackTask(oldLeaderAddress);
            }catch (Exception e){
                log.error("selectNewLeader()->deleteOldLeaderBackTask",e);
            }

        }
    }
}
