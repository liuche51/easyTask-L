package com.github.liuche51.easyTask.cluster;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dao.DBMonitor;
import com.github.liuche51.easyTask.dao.SQLliteMultiPool;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.dto.proto.ResultDto;
import com.github.liuche51.easyTask.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTask.netty.client.NettyClient;
import com.github.liuche51.easyTask.util.StringConstant;
import com.github.liuche51.easyTask.util.StringUtils;
import com.github.liuche51.easyTask.util.Util;

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

    public static Map<String, Map<String, List>> getDBTraceInfoByTransactionId(String tranId) throws Exception {
        Map<String, Map<String, List>> map = new HashMap<>(3);
        Map<String, List> leaderInfo = DBMonitor.getInfoByTransactionId(tranId);
        map.put(AnnularQueue.getInstance().getConfig().getAddress(), leaderInfo);
        Iterator<Node> items = ClusterService.CURRENTNODE.getFollows().iterator();
        while (items.hasNext()) {
            Node item = items.next();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(Util.generateIdentityId()).setInterfaceName(NettyInterfaceEnum.GET_DBINFO_BY_TRANSACTIONID).setSource(AnnularQueue.getInstance().getConfig().getAddress())
                    .setBody(tranId);
            NettyClient client = item.getClientWithCount(AnnularQueue.getInstance().getConfig().getTryCount());
            Object ret = client.sendSyncMsg(builder.build());
            Dto.Frame frame = (Dto.Frame) ret;
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (result!=null&&StringConstant.TRUE.equals(result.getResult())
                    &&!StringUtils.isNullOrEmpty(result.getBody())) {
                Map<String, List> followInfo = JSONObject.parseObject(result.getBody(), new TypeReference<Map<String, List>>() {});
                map.put(item.getAddress(), followInfo);
            }else
                continue;
        }
        return map;
    }
}
