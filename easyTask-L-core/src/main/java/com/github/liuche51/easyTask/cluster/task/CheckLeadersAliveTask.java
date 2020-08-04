package com.github.liuche51.easyTask.cluster.task;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.follow.VoteLeader;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.util.DateUtils;
import com.github.liuche51.easyTask.util.StringConstant;
import com.github.liuche51.easyTask.zk.ZKService;

import java.util.Iterator;
import java.util.Map;
/**
 * 节点对zk的心跳。检查leader是否失效。
 * 失效则进入选举
 */
public class CheckLeadersAliveTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                Map<String, Node> leaders = ClusterService.CURRENTNODE.getLeaders();
                Iterator<Map.Entry<String, Node>> items = leaders.entrySet().iterator();//使用遍历+移除操作安全的迭代器方式
                while (items.hasNext()) {
                    Map.Entry<String, Node> item = items.next();
                    String path = StringConstant.CHAR_SPRIT + item.getValue().getAddress();
                    ZKNode node = ZKService.getDataByPath(path);
                    if (node == null)//防止leader节点已经不在zk。
                    {
                        log.info("heartBeatToLeader():leader is not exist in zk,so to selectNewLeader.");
                        VoteLeader.selectNewLeader(node, item.getValue().getAddress());
                        items.remove();
                        continue;
                    }
                    //如果最后心跳时间超过60s，则直接删除该节点信息。并从自己的leader集合中移除掉
                    if (DateUtils.isGreaterThanLoseTime(node.getLastHeartbeat(),item.getValue().getClockDiffer().getDifferSecond())) {
                        items.remove();
                        ZKService.deleteNodeByPathIgnoreResult(path);
                    }
                    //如果最后心跳时间超过30s，进入选举新leader流程。并从自己的leader集合中移除掉
                    else if (DateUtils.isGreaterThanDeadTime(node.getLastHeartbeat(),item.getValue().getClockDiffer().getDifferSecond())) {
                        log.info("heartBeatToLeader():start to selectNewLeader");
                        VoteLeader.selectNewLeader(node, item.getValue().getAddress());
                        items.remove();
                    }

                }
            } catch (Exception e) {
                log.error("heartBeatToLeader()", e);
            }
            try {
                Thread.sleep(AnnularQueue.getInstance().getConfig().getHeartBeat());
            } catch (InterruptedException e) {
                log.error("heartBeatToLeader()", e);
            }
        }
    }
}
