package com.github.liuche51.easyTask.register;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ZKService {
    private static Logger log = LoggerFactory.getLogger(ZKService.class);

    /**
     * 当前节点注册为永久节点
     *
     * @param data
     */
    public static void register(ZKNode data) {
        try {
            String path = "/" + EasyTaskConfig.getInstance().getzKServerName();// Util.getLocalIP();
           //检查是否存在节点
            Stat stat1 = ZKUtil.getClient().checkExists().forPath(path);
            if (stat1 != null) {
                ZKUtil.getClient().setData().forPath(path,JSONObject.toJSONString(data).getBytes());//重新覆盖注册信息
                return;
            } else {
                //创建永久节点
                ZKUtil.getClient().create().withMode(CreateMode.PERSISTENT).forPath(path, JSONObject.toJSONString(data).getBytes());
            }
        } catch (Exception e) {
            log.error("register exception！", e);
        }
    }

    /**
     * 获取命名空间下的的子节点信息
     *
     * @return
     */
    public static List<String> getChildrenByNameSpase() {
        String path = "/";
        return getChildrenByPath(path);
    }

    /**
     * 获取当前节点下的子节点信息
     *
     * @return
     */
    public static List<String> getChildrenByCurrentNode() {
        String path = "/" + EasyTaskConfig.getInstance().getzKServerName();
        return getChildrenByPath(path);
    }
    /**
     * 根据节点路径，获取节点下的子节点名称
     * @param path
     * @return
     */
    public static List<String> getChildrenByPath(String path) {
        try {
            List<String> list = ZKUtil.getClient().getChildren().forPath(path);
            return list;
        } catch (Exception e) {
            log.error("getChildrenByPath exception!", e);
        }
        return null;
    }
    /**
     * 获取当前节点的值信息
     *
     * @return
     */
    public static ZKNode getDataByCurrentNode() {
        String path = "/" + EasyTaskConfig.getInstance().getzKServerName();
        return getDataByPath(path);
    }

    /**
     * 根据节点路径，获取节点值信息
     * @param path
     * @return
     */
    public static ZKNode getDataByPath(String path) {
        try {
            byte[] bytes = ZKUtil.getClient().getData().forPath(path);
            return JSONObject.parseObject(bytes,ZKNode.class);
        } catch (Exception e) {
            log.error("getDataByPath exception!", e);
        }
        return null;
    }
}
