package com.github.liuche51.easyTask.register;

import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.core.Util;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RegisterCenter {
    private static Logger log = LoggerFactory.getLogger(RegisterCenter.class);

    public static void register(NodeData data) {
        try {
            String path = "/" + EasyTaskConfig.getInstance().getzKServerName() + "/" + Util.getLocalIP();
            //创建临时节点
            ZKUtil.getClient().create().withMode(CreateMode.EPHEMERAL).forPath(path, data.toString().getBytes());
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static   List<String> getList() {
        String path = "/" +  EasyTaskConfig.getInstance().getzKServerName();
        try {
            List<String> list = ZKUtil.getClient().getChildren().forPath(path);
            return list;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

}
