package com.github.liuche51.easyTask.cluster;

import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.core.EasyTaskConfig;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

/**
 * 节点对象
 */
public class Node {
    private String host;
    private int port= EasyTaskConfig.getInstance().getServerPort();
    private List<Node> follows=new LinkedList<>();
    private List<Node> leaders=new LinkedList<>();
    private NettyClient client;
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

    public List<Node> getLeaders() {
        return leaders;
    }

    public void setLeaders(List<Node> leaders) {
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
    private void buildConnect(){
        this.client=new NettyClient(new InetSocketAddress(host,port));
    }
}
