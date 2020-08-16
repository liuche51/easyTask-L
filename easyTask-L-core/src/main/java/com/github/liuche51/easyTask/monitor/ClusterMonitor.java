package com.github.liuche51.easyTask.monitor;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dao.SQLliteMultiPool;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.dto.proto.ResultDto;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTask.netty.client.NettyClient;
import com.github.liuche51.easyTask.netty.client.NettyConnectionFactory;
import com.github.liuche51.easyTask.netty.client.NettyMsgService;
import com.github.liuche51.easyTask.util.StringConstant;
import com.github.liuche51.easyTask.util.StringUtils;
import com.github.liuche51.easyTask.util.Util;
import com.github.liuche51.easyTask.zk.ZKService;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 集群信息监控类
 */
public class ClusterMonitor {
    /**
     * 获取当前节点的集群信息
     *
     * @return
     */
    public static String getCurrentNodeInfo() {
        return JSONObject.toJSONString(ClusterService.CURRENTNODE);
    }

    public static String getSqlitePoolInfo() {
        StringBuilder str = new StringBuilder();
        Map<String, ConcurrentLinkedQueue<Connection>> map = SQLliteMultiPool.getInstance().getPools();
        for (Map.Entry<String, ConcurrentLinkedQueue<Connection>> item : map.entrySet()) {
            str.append(item.getKey()).append(":count=" + item.getValue().size()).append(",");
        }
        return str.toString();
    }

    public static Map<String, Map<String, List>> getDBTraceInfoByTaskId(String taskId) throws Exception {
        Map<String, Map<String, List>> map = new HashMap<>(3);
        Map<String, List> leaderInfo = DBMonitor.getInfoByTaskId(taskId);
        map.put(AnnularQueue.getInstance().getConfig().getAddress(), leaderInfo);
        Iterator<Node> items = ClusterService.CURRENTNODE.getFollows().iterator();
        while (items.hasNext()) {
            Node item = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.GET_DBINFO_BY_TASKID).setSource(AnnularQueue.getInstance().getConfig().getAddress())
                    .setBody(taskId);
            NettyClient client = item.getClientWithCount(AnnularQueue.getInstance().getConfig().getTryCount());
            Object ret = NettyMsgService.sendSyncMsg(client,builder.build());
            Dto.Frame frame = (Dto.Frame) ret;
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (result != null && StringConstant.TRUE.equals(result.getResult())
                    && !StringUtils.isNullOrEmpty(result.getBody())) {
                Map<String, List> followInfo = JSONObject.parseObject(result.getBody(), new TypeReference<Map<String, List>>() {
                });
                map.put(item.getAddress(), followInfo);
            } else
                continue;
        }
        return map;
    }

    public static ZKNode getCurrentZKNodeInfo() throws Exception {
        return ZKService.getDataByCurrentNode();
    }
    public static Map<String, String> getNettyClientPoolInfo(){
        Map<String, String> map=new HashMap<>(AnnularQueue.getInstance().getConfig().getBackupCount());
        Map<String, ConcurrentLinkedQueue<NettyClient>> pools=NettyConnectionFactory.getInstance().getPools();
        Iterator<Map.Entry<String, ConcurrentLinkedQueue<NettyClient>>> items=pools.entrySet().iterator();
        while (items.hasNext()){
            Map.Entry<String, ConcurrentLinkedQueue<NettyClient>> item=items.next();
            ConcurrentLinkedQueue<NettyClient> v=item.getValue();
            StringBuilder builder=new StringBuilder();
            builder.append("availableQty:").append(v.size());
            map.put(item.getKey(),builder.toString());
        }
        return map;
    }
}
