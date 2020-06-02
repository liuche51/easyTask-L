package com.github.liuche51.easyTask.cluster;

import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.dto.proto.Dto;
import org.apache.log4j.Logger;

public class ClusterUtil {
    private static final Logger log = Logger.getLogger(ClusterUtil.class);
    /**
     * 带重试次数的同步消息发送
     *
     * @param client
     * @param msg
     * @param tryCount
     * @return
     */
    public static boolean sendSyncMsgWithCount(NettyClient client, Object msg, int tryCount) {
        if (tryCount == 0) return false;
        try {
            Object ret = client.sendSyncMsg(msg);
            Dto.Frame frame = (Dto.Frame) ret;
            String body = frame.getBody();
            if ("true".equals(body))
                return true;
            tryCount--;
        } catch (Exception e) {
            tryCount--;
        }
        log.debug("sendSyncMsgWithCount tryCount="+tryCount);
        return sendSyncMsgWithCount(client, msg, tryCount);

    }
}
