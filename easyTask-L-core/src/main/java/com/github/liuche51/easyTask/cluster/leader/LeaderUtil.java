package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.register.ZKService;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

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
    public static List<Node> selectNewFollows() {
        List<String> list = ZKService.getChildrenByNameSpase();
        //排除自己
        Optional<String> temp=list.stream().filter(x->x.equals(EasyTaskConfig.getInstance().getzKServerName())).findFirst();
        if(temp.isPresent())
            list.remove(temp.get());

        List<Node> follows = new LinkedList<>();
        int count= EasyTaskConfig.getInstance().getBackupCount();
        if (list.size() == 0) return follows;
        else if (list.size() == 1&&count>0) {
            //follows.add(list.get(0));
            return follows;
        } else {
            Random random = new Random();
            for (int i = 0; i <count; i++) {
                int index = random.nextInt(list.size() + 1);
                //follows.add(list.get(index));
                list.remove(index);
            }
        }
        return follows;
    }
}
