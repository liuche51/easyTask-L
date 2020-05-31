package com.github.liuche51.easyTask.cluster;

import com.alibaba.fastjson.annotation.JSONField;
import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.core.EasyTaskConfig;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 节点对象
 */
public class Node implements Serializable {
    private String host="";
    private int port= EasyTaskConfig.getInstance().getServerPort();
    /**
     * 当前节点的所有follows
     */
    private List<Node> follows=new LinkedList<>();
    /**
     * 当前节点的所有leader
     */
    private Map<String,Node> leaders=new HashMap<>();
    /**
     * 当前节点的客户端连接。
     */
    @JSONField(serialize = false)
    private NettyClient client;
    public Node(String host,int port){
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

    public List<Node> getFollows() {
        return follows;
    }

    public void setFollows(List<Node> follows) {
        this.follows = follows;
    }

    public  Map<String,Node> getLeaders() {
        return leaders;
    }

    public void setLeaders(Map<String,Node> leaders) {
        this.leaders = leaders;
    }

    public NettyClient getClient() {
        if(client==null)
            buildConnect();
        return client;
    }

    public void setClient(NettyClient client) {
        this.client = client;
    }

    /**
     * 构建到当前节点的客户端连接
     */
    private void buildConnect(){
        this.client=new NettyClient(new InetSocketAddress(host,port));
    }
}