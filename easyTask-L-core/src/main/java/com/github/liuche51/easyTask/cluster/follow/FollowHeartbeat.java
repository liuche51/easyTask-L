package com.github.liuche51.easyTask.cluster.follow;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.dto.zk.ZKHost;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.zk.ZKService;
import com.github.liuche51.easyTask.util.DateUtils;
import com.github.liuche51.easyTask.util.StringConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FollowHeartbeat {
    private static final Logger log = LoggerFactory.getLogger(LeaderService.class);
    /**
     * 节点对zk的心跳。检查leader是否失效。
     * 失效则进入选举
     */
    public static void heartBeatToLeader() {
        Thread th1 = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Map<String, Node> leaders = ClusterService.CURRENTNODE.getLeaders();
                        Iterator<Map.Entry<String, Node>> items = leaders.entrySet().iterator();//使用遍历+移除操作安全的迭代器方式
                       while (items.hasNext()){
                           Map.Entry<String, Node> item=items.next();
                            String path = StringConstant.CHAR_SPRIT + item.getValue().getAddress();
                            ZKNode node = ZKService.getDataByPath(path);
                            //如果最后心跳时间超过60s，则直接删除该节点信息。并从自己的leader集合中移除掉
                            if (ZonedDateTime.now().minusSeconds(EasyTaskConfig.getInstance().getDeleteZKTimeOunt())
                                    .compareTo(DateUtils.parse(node.getLastHeartbeat())) > 0) {
                                items.remove();
                                ZKService.deleteNodeByPathIgnoreResult(path);
                            }
                            //如果最后心跳时间超过30s，进入选举新leader流程。并从自己的leader集合中移除掉
                            else if (ZonedDateTime.now().minusSeconds(EasyTaskConfig.getInstance().getSelectLeaderZKNodeTimeOunt())
                                    .compareTo(DateUtils.parse(node.getLastHeartbeat())) > 0) {
                                log.info("heartBeatToLeader():start to selectNewLeader");
                                VoteLeader.selectNewLeader(node,item.getValue().getAddress());
                                items.remove();
                            }

                        }
                    } catch (Exception e) {
                        log.error( "heartBeatToLeader()",e);
                    }
                    try {
                        Thread.sleep(EasyTaskConfig.getInstance().getHeartBeat());
                    } catch (InterruptedException e) {
                        log.error("heartBeatToLeader()",e);
                    }
                }
            }
        });
        th1.start();
    }
}