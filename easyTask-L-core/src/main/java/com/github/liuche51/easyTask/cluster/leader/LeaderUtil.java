package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.dto.proto.Dto;

import java.util.LinkedList;
import java.util.List;

/**
 * leader类
 */
public class LeaderUtil {

    /**
     * 数据备份的跟随者
     */
    public static List<NettyClient> FOLLOWS=new LinkedList<>();
    /**
     * 带重试次数的消息发送
     *
     * @param client
     * @param msg
     * @param tryCount
     * @return
     */
    public static boolean sendSyncMsgWithCount(NettyClient client, Object msg, int tryCount) {
        if (tryCount == 0) return false;
        try {
            tryCount--;
            Object ret = client.sendSyncMsg(msg);
            Dto.Frame frame = (Dto.Frame) ret;
            String body = frame.getBody();
            if ("true".equals(body))
                return true;
        } catch (Exception e) {
            tryCount--;
        }
        return sendSyncMsgWithCount(client, msg, tryCount);

    }
}
