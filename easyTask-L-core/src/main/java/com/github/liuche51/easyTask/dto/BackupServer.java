package com.github.liuche51.easyTask.dto;

/**
 * 数据备份的服务器
 */
public class BackupServer {
    private Long id;
    private String server;
    private String createTime;
    public BackupServer(){
    }
    public BackupServer(String server){
        this.server=server;
    }
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
