package com.github.liuche51.easyTask.cluster;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.dao.SQLliteMultiPool;

import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 集群信息监控类
 */
public class ClusterMonitor {
    /**
     * 获取当前节点的集群信息
     * @return
     */
    public static String getCurrentNodeInfo(){
        return JSONObject.toJSONString(ClusterService.CURRENTNODE);
    }
    public static String getSqlitePoolInfo(){
        StringBuilder str=new StringBuilder();
        Map<String, ConcurrentLinkedQueue<Connection>> map= SQLliteMultiPool.getInstance().getPools();
        for (Map.Entry<String, ConcurrentLinkedQueue<Connection>> item:map.entrySet()){
            str.append(item.getKey()).append(":count="+item.getValue().size()).append(",");
        }
        return str.toString();
    }
}
