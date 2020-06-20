package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.core.Util;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.register.ZKService;
import com.github.liuche51.easyTask.util.DateUtils;
import com.github.liuche51.easyTask.util.StringConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;

public class LeaderHeartbeat {
    private static final Logger log = LoggerFactory.getLogger(LeaderHeartbeat.class);
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
                            ZKService.register(new ZKNode(ClusterService.CURRENTNODE.getHost(), ClusterService.CURRENTNODE.getPort()));
                            LeaderService.initSelectFollows();
                        }
                    } catch (Exception e) {
                        log.error("",e);
                    }
                    try {
                        Thread.sleep(EasyTaskConfig.getInstance().getHeartBeat());
                    } catch (InterruptedException e) {
                        log.error("",e);
                    }
                }
            }
        });
        th1.start();
    }
    /**
     * 节点对zk的心跳。检查follows是否失效。
     * 失效则进入选举
     */
    public static void heartBeatToFollow() {
        Thread th1 = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        List<Node> follows = ClusterService.CURRENTNODE.getFollows();
                        Iterator<Node> items=follows.iterator();
                       while (items.hasNext()){
                           Node oldFollow=items.next();
                           String path = StringConstant.CHAR_SPRIT + oldFollow.getAddress();
                           ZKNode node = ZKService.getDataByPath(path);
                           if(node==null)//防止follow节点已经不在zk。导致不能重新选举
                           {
                               log.info("heartBeatToFollow():oldFollow is not exist in zk,so to selectNewFollow.");
                               VoteFollows.selectNewFollow(oldFollow);
                               continue;
                           }
                           //如果最后心跳时间超过60s，则直接删除该节点信息。
                           if (ZonedDateTime.now().minusSeconds(EasyTaskConfig.getInstance().getDeleteZKTimeOunt())
                                   .compareTo(DateUtils.parse(node.getLastHeartbeat())) > 0) {
                               ZKService.deleteNodeByPathIgnoreResult(path);
                           }
                           //如果最后心跳时间超过30s，进入选举新leader流程
                           else if (ZonedDateTime.now().minusSeconds(EasyTaskConfig.getInstance().getSelectLeaderZKNodeTimeOunt())
                                   .compareTo(DateUtils.parse(node.getLastHeartbeat())) > 0) {
                               log.info("heartBeatToFollow():start to selectNewFollow");
                               VoteFollows.selectNewFollow(oldFollow);
                           }

                        }
                    } catch (Exception e) {
                        log.error( "",e);
                    }
                    try {
                        Thread.sleep(EasyTaskConfig.getInstance().getHeartBeat());
                    } catch (InterruptedException e) {
                        log.error("",e);
                    }
                }
            }
        });
        th1.start();
    }
}
