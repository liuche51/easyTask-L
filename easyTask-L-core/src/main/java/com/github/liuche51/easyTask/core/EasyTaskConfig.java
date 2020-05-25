package com.github.liuche51.easyTask.core;

import com.github.liuche51.easyTask.backup.server.NettyServer;
import com.github.liuche51.easyTask.register.ZKUtil;

import java.io.File;

/**
 * 系统配置项
 */
public class EasyTaskConfig {
    private static EasyTaskConfig singleton = null;

    public static EasyTaskConfig getInstance() {
        if (singleton == null) {
            synchronized (EasyTaskConfig.class) {
                if (singleton == null) {
                    singleton = new EasyTaskConfig();
                }
            }
        }
        return singleton;
    }

    private EasyTaskConfig() {
    }

    /**
     * 任务备份数量，默认1。最大2，超过部分无效
     */
    private int backupCount = 1;
    /**
     * 自定义任务本地存储路径。默认以当前项目根路径
     */
    private String taskStorePath;
    /**
     * sqlite连接池大小设置。默认cpu数的两倍
     */
    private int sQLlitePoolSize = Runtime.getRuntime().availableProcessors() * 2;
    /**
     * 设置当前服务的名称。请保持唯一性。
     */
    private String zKServerName = "";
    /**
     * 设置当前easyTask组件的集群服务端口号。默认2020
     */
    private int serverPort = 2020;
    /**
     * 设置集群通信调用超时时间。默认3秒
     */
    private int timeOut=3;
    /**
     * 是否开启为集群模式。方便单机测试使用。
     * 单机环境，需要服务端口设置为不同。指定的db文件夹也不要相同。IP相同没问题
     */
    private boolean enablePseudoCluster=false;

    public int getBackupCount() {
        return backupCount;
    }

    public void setBackupCount(int backupCount) throws Exception {
        if (AnnularQueue.isRunning)
            throw new Exception("please before AnnularQueue started set");
        if (backupCount < 0) this.backupCount = 0;
        else if (backupCount > 2) this.backupCount = 2;
        else this.backupCount = backupCount;
    }

    public String getTaskStorePath() {
        return taskStorePath;
    }

    /**
     * set Task Store Path.example  C:\\db
     *
     * @param path
     * @throws Exception
     */
    public void setTaskStorePath(String path) throws Exception {
        if (AnnularQueue.isRunning)
            throw new Exception("please before AnnularQueue started set");
        this.taskStorePath = path + "\\easyTask.db";
        File file = new File(path);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    public int getsQLlitePoolSize() {
        return sQLlitePoolSize;
    }

    /**
     * set SQLlitePool Size，default qty 15
     *
     * @param count
     * @throws Exception
     */
    public void setSQLlitePoolSize(int count) throws Exception {
        if (AnnularQueue.isRunning)
            throw new Exception("please before AnnularQueue started set");
        if (count < 1)
            throw new Exception("poolSize must >1");
        this.sQLlitePoolSize = count;
    }

    public String getzKServerName() {
        return zKServerName;
    }

    /**
     * set ZKServerName，default qty 15
     *
     * @param name
     * @throws Exception
     */
    public void setZKServerName(String name) throws Exception {
        if (AnnularQueue.isRunning)
            throw new Exception("please before AnnularQueue started set");
        if (name == null || "".equals(name))
            throw new Exception("ZK_SERVER_NAME must not empty");
        this.zKServerName = name;
    }

    public int getServerPort() {
        return serverPort;
    }

    /**
     * set ServerPort，default 2020
     *
     * @param port
     * @throws Exception
     */
    public void setServerPort(int port) throws Exception {
        if (AnnularQueue.isRunning)
            throw new Exception("please before AnnularQueue started set");
        if (port == 0)
            throw new Exception("ServerPort must not empty");
        this.serverPort = port;
    }

    public int getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(int timeOut) {
        this.timeOut = timeOut;
    }

    public boolean isEnablePseudoCluster() {
        return enablePseudoCluster;
    }

    public void setEnablePseudoCluster(boolean enablePseudoCluster) {
        this.enablePseudoCluster = enablePseudoCluster;
    }
}
