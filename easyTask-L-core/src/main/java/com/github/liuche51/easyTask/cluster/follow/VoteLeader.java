package com.github.liuche51.easyTask.cluster.follow;

import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.dto.zk.ZKHost;
import com.github.liuche51.easyTask.dto.zk.ZKNode;

import java.util.List;

public class VoteLeader {
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
            List<ScheduleBak> baks= ScheduleBakDao.getBySource(oldLeaderAddress);
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