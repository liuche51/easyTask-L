package com.github.liuche51.easyTask.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 系统配置项
 */
public class EasyTaskConfig {
    private static final Logger log = LoggerFactory.getLogger(EasyTaskConfig.class);
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
     * 前服务的名称。IP+端口号
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
    /**
     * ZK节点信息更新超过60s就判断为失效节点，任何其他节点可删除掉
     */
    private int loseTimeOut=60;
    /**
     * ZK节点信息更新超过30s就判断为Leader失效节点，其Follow节点可进入选举新Leader
     */
    private int deadTimeOut=30;
    /**
     * 节点对zk的心跳频率。默认2s一次
     */
    private int heartBeat=2;
    /**
     * 集群通信失败重试次数。默认2次
     */
    private int tryCount=2;
    /**
     * 清理任务备份表中失效的leader备份。默认1小时一次。单位毫秒
     */
    private int clearScheduleBakTime=36500000;
    /**
     * 集群总线程池
     */
    private ExecutorService clusterPool = null;

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
        try {
            StringBuffer buffer=new StringBuffer(Util.getLocalIP());
            buffer.append(":").append(getServerPort());
            return buffer.toString();
        }catch (Exception e){
            log.error("",e);
        }
        return null;
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

    public int getLoseTimeOut() {
        return loseTimeOut;
    }

    public void setLoseTimeOut(int loseTimeOut) {
        this.loseTimeOut = loseTimeOut;
    }

    public int getDeadTimeOut() {
        return deadTimeOut;
    }

    public void setDeadTimeOut(int deadTimeOut) {
        this.deadTimeOut = deadTimeOut;
    }

    public int getHeartBeat() {
        return heartBeat*1000;
    }

    public void setHeartBeat(int heartBeat) {
        this.heartBeat = heartBeat;
    }

    public int getTryCount() {
        return tryCount;
    }

    public void setTryCount(int tryCount) {
        this.tryCount = tryCount;
    }

    public int getClearScheduleBakTime() {
        return clearScheduleBakTime;
    }

    public void setClearScheduleBakTime(int clearScheduleBakTime) {
        this.clearScheduleBakTime = clearScheduleBakTime;
    }

    public ExecutorService getClusterPool() {
        if(this.clusterPool==null)
            this.clusterPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        return clusterPool;
    }

    /**
     * 设置集群总线程池
     * @param clusterPool
     * @throws Exception
     */
    public void setClusterPool(ThreadPoolExecutor clusterPool) throws Exception {
        if (AnnularQueue.isRunning)
            throw new Exception("please before AnnularQueue started set");
        this.clusterPool = clusterPool;
    }
}
