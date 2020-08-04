package com.github.liuche51.easyTask.cluster.task;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.dto.ClockDiffer;

import java.time.ZonedDateTime;
import java.util.*;

/**
 * 同步与其他关联节点的时钟差定时任务
 */
public class NodeClockAdjustTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                Map<String, Node> leaders = ClusterService.CURRENTNODE.getLeaders();
                Iterator<Map.Entry<String, Node>> items = leaders.entrySet().iterator();
                while (items.hasNext()) {
                    Map.Entry<String, Node> item = items.next();
                    ClockDiffer differ = item.getValue().getClockDiffer();
                    dealSyncObjectNodeClockDiffer(item.getValue(), differ);
                }
                List<Node> follows = ClusterService.CURRENTNODE.getFollows();
                Iterator<Node> items2 = follows.iterator();
                while (items2.hasNext()) {
                    Node item = items2.next();
                    ClockDiffer differ = item.getClockDiffer();
                    dealSyncObjectNodeClockDiffer(item, differ);
                }
            } catch (ConcurrentModificationException e) {
                //多线程并发导致items.next()异常，但是没啥太大影响(影响后续元素迭代)。可以直接忽略
                log.error("normally exception error.can ignore." + e.getMessage());
            } catch (Exception e) {
                log.error("submitNewTaskByOldLeader()->", e);
            } finally {
                try {
                    Thread.sleep(300000l);//5分钟执行一次
                } catch (InterruptedException e) {
                    log.error("", e);
                }
            }
        }

    }

    private void dealSyncObjectNodeClockDiffer(Node node, ClockDiffer differ) {
        //如果还没有同步过时钟差或距离上次同步已经过去5分钟了，则重新同步一次
        if (!differ.isHasSync() || ZonedDateTime.now().minusMinutes(5)
                .compareTo(differ.getLastSyncDate()) > 0) {
            ClusterService.syncObjectNodeClockDiffer(Arrays.asList(node), AnnularQueue.getInstance().getConfig().getTryCount());
        }
    }
}
