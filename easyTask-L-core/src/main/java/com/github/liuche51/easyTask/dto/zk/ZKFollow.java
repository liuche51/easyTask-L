package com.github.liuche51.easyTask.dto.zk;

import com.github.liuche51.easyTask.core.EasyTaskConfig;

public class ZKFollow {
    private String host;
    private int port= EasyTaskConfig.getInstance().getServerPort();
    public ZKFollow(String host) {
        this.host = host;
    }
    public ZKFollow(String host, int port) {
        this.host = host;
        this.port = port;
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
}
