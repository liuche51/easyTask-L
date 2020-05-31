package com.github.liuche51.easyTask.cluster;

import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.core.Util;
import com.github.liuche51.easyTask.dto.zk.ZKHost;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.register.ZKService;
import com.github.liuche51.easyTask.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public class ClusterService {
    private static Logger log = LoggerFactory.getLogger(ClusterService.class);
    /**
     * 集群总线程池
     */
    public static ExecutorService CLUSTERPOOL = null;
    /**
     * 当前集群节点的Node对象
     */
    public static Node CURRENTNODE;

    /**
     * 初始化当前节点。
     * zk注册，选follows,开始心跳
     *这里不考虑短暂宕机重启继续使用原follows情况。让原follows等待超时后重新选举leader就好了
     * @return
     */
    public static boolean initCurrentNode() {
        try {
            CURRENTNODE = new Node(Util.getLocalIP(), EasyTaskConfig.getInstance().getServerPort());
            ZKNode node = new ZKNode(CURRENTNODE.getHost(), CURRENTNODE.getPort());
            node.setCreateTime(DateUtils.getCurrentDateTime());
            node.setLastHeartbeat(DateUtils.getCurrentDateTime());
            ZKService.register(node);
            LeaderService.initSelectFollows();
            node.setFollows(Util.nodeToZKHost(CURRENTNODE.getFollows()));
            ZKService.setDataByCurrentNode(node);
            heartBeatToZK();
            return true;
        } catch (UnknownHostException e) {
            log.error("", e);
        }
        return false;
    }

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
                            ZKService.register(new ZKNode(CURRENTNODE.getHost(), CURRENTNODE.getPort()));
                            LeaderService.initSelectFollows();
                        }
                        Thread.sleep(EasyTaskConfig.getInstance().getHeartBeat());
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
            }
        });
        th1.start();
    }

}
