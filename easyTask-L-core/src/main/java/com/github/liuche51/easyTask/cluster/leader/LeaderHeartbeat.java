package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.core.Util;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.util.exception.VotedException;
import com.github.liuche51.easyTask.util.exception.VotingException;
import com.github.liuche51.easyTask.zk.ZKService;
import com.github.liuche51.easyTask.util.DateUtils;
import com.github.liuche51.easyTask.util.StringConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.ConcurrentModificationException;
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
     * 失效则进入选举。选举后将原follow备份数据同步给新follow
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
                               VoteFollows.selectNewFollow(oldFollow,items);
                               continue;
                           }
                           //如果最后心跳时间超过60s，则直接删除该节点信息。
                           if (ZonedDateTime.now().minusSeconds(EasyTaskConfig.getInstance().getDeleteZKTimeOunt())
                                   .compareTo(DateUtils.parse(node.getLastHeartbeat())) > 0) {
                               ZKService.deleteNodeByPathIgnoreResult(path);
                           }
                           //如果最后心跳时间超过30s，进入选举新follow流程
                           else if (ZonedDateTime.now().minusSeconds(EasyTaskConfig.getInstance().getSelectLeaderZKNodeTimeOunt())
                                   .compareTo(DateUtils.parse(node.getLastHeartbeat())) > 0) {
                               log.info("heartBeatToFollow():start to selectNewFollow");
                               Node newFollow=VoteFollows.selectNewFollow(oldFollow,items);
                               log.info("heartBeatToFollow():start to syncDataToNewFollow");
                               LeaderService.syncDataToNewFollow(oldFollow,newFollow);
                           }

                        }
                    }
                    catch (ConcurrentModificationException e){
                        //多线程并发导致items.next()异常，但是没啥太大影响(影响后续元素迭代)。可以直接忽略
                        log.info("normally exception error.can ignore.{}",e.getMessage());
                    }
                    catch (VotingException e){
                        //异常导致选新follow。但此时刚好有其他地方触发正在选举中。
                        //心跳这里就没必要继续触发选新follow了
                        log.info("normally exception error.can ignore.{}",e.getMessage());
                    }
                    catch (VotedException e){
                        //原因同上VotingException
                        log.info("normally exception error.can ignore.{}",e.getMessage());
                    }
                    catch (Exception e) {
                        log.error( "heartBeatToFollow()",e);
                    }
                    try {
                        Thread.sleep(EasyTaskConfig.getInstance().getHeartBeat());
                    } catch (InterruptedException e) {
                        log.error("heartBeatToFollow()",e);
                    }
                }
            }
        });
        th1.start();
    }
}
