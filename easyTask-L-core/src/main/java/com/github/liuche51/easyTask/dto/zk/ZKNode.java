package com.github.liuche51.easyTask.dto.zk;

import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;

import java.util.LinkedList;
import java.util.List;

public class ZKNode {
    private String host;
    private int port= AnnularQueue.getInstance().getConfig().getServerPort();
    /**
     * 最近一次心跳时间
     */
    private String lastHeartbeat;
    private String createTime;
    /**
     * follows
     */
    private List<ZKHost> follows=new LinkedList<>();
    /**
     * leaders
     */
    private List<ZKHost> leaders=new LinkedList<>();
    public ZKNode(){}
    public ZKNode(String host,int port){
        this.host=host;
        this.port=port;
    }

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

    public List<ZKHost> getFollows() {
        return follows;
    }

    public void setFollows(List<ZKHost> follows) {
        this.follows = follows;
    }

    public List<ZKHost> getLeaders() {
        return leaders;
    }

    public void setLeaders(List<ZKHost> leaders) {
        this.leaders = leaders;
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
