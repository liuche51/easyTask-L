package com.github.liuche51.easyTask.dto.zk;

import com.github.liuche51.easyTask.core.EasyTaskConfig;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class ZKNode {
    private String host;
    private int port= EasyTaskConfig.getInstance().getServerPort();
    /**
     * follows
     */
    private List<ZKFollow> follows=new LinkedList<>();
    public ZKNode(String host,int port){}
    /**
     * 最近一次心跳时间
     */
    private String lastHeartbeat;
    private String createTime;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public List<ZKFollow> getFollows() {
        return follows;
    }

    public void setFollows(List<ZKFollow> follows) {
        this.follows = follows;
    }

    public String getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(String lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
