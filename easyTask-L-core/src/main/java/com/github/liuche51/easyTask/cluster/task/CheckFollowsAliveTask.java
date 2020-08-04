package com.github.liuche51.easyTask.cluster.task;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.cluster.leader.VoteFollows;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.util.DateUtils;
import com.github.liuche51.easyTask.util.StringConstant;
import com.github.liuche51.easyTask.util.exception.VotedException;
import com.github.liuche51.easyTask.util.exception.VotingException;
import com.github.liuche51.easyTask.zk.ZKService;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
/**
 * 节点对zk的心跳。检查follows是否失效。
 * 失效则进入选举。选举后将原follow备份数据同步给新follow
 */
public class CheckFollowsAliveTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                List<Node> follows = ClusterService.CURRENTNODE.getFollows();
                Iterator<Node> items = follows.iterator();
                while (items.hasNext()) {
                    Node oldFollow = items.next();
                    String path = StringConstant.CHAR_SPRIT + oldFollow.getAddress();
                    ZKNode node = ZKService.getDataByPath(path);
                    if (node == null)//防止follow节点已经不在zk。导致不能重新选举
                    {
                        log.info("heartBeatToFollow():oldFollow is not exist in zk,so to selectNewFollow.");
                        VoteFollows.selectNewFollow(oldFollow, items);
                        continue;
                    }
                    //如果最后心跳时间超过60s，则直接删除该节点信息。
                    if (DateUtils.isGreaterThanLoseTime(node.getLastHeartbeat(),oldFollow.getClockDiffer().getDifferSecond())) {
                        ZKService.deleteNodeByPathIgnoreResult(path);
                    }
                    //如果最后心跳时间超过30s，进入选举新follow流程
                    else if (DateUtils.isGreaterThanDeadTime(node.getLastHeartbeat(),oldFollow.getClockDiffer().getDifferSecond())) {
                        log.info("heartBeatToFollow():start to selectNewFollow");
                        Node newFollow = VoteFollows.selectNewFollow(oldFollow, items);
                        log.info("heartBeatToFollow():start to syncDataToNewFollow");
                        LeaderService.syncDataToNewFollow(oldFollow, newFollow);
                    }

                }
            } catch (ConcurrentModificationException e) {
                //多线程并发导致items.next()异常，但是没啥太大影响(影响后续元素迭代)。可以直接忽略
                log.error("normally exception error.can ignore."+e.getMessage());
            } catch (VotingException e) {
                //异常导致选新follow。但此时刚好有其他地方触发正在选举中。
                //心跳这里就没必要继续触发选新follow了
                log.error("normally exception error.can ignore."+e.getMessage());
            } catch (VotedException e) {
                //原因同上VotingException
                log.error("normally exception error.can ignore."+e.getMessage());
            } catch (Exception e) {
                log.error("heartBeatToFollow()", e);
            }
            try {
                Thread.sleep(AnnularQueue.getInstance().getConfig().getHeartBeat());
            } catch (InterruptedException e) {
                log.error("heartBeatToFollow()", e);
            }
        }
    }
}
