package com.github.liuche51.easyTask.cluster;

import com.alibaba.fastjson.JSONObject;

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
}
