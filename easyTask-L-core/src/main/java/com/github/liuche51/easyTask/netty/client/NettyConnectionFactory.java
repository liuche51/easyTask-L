package com.github.liuche51.easyTask.netty.client;

import com.github.liuche51.easyTask.core.AnnularQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Netty客户端连接池工厂
 */
public class NettyConnectionFactory {
    private static final Logger log = LoggerFactory.getLogger(NettyConnectionFactory.class);
    private Map<String, ConcurrentLinkedQueue<NettyClient>> pools = new HashMap<>(2);
    private ReentrantLock lock = new ReentrantLock();
    private static NettyConnectionFactory singleton = null;

    public static NettyConnectionFactory getInstance() {
        if (singleton == null) {
            synchronized (NettyConnectionFactory.class) {
                if (singleton == null) {
                    singleton = new NettyConnectionFactory();
                }
            }
        }
        return singleton;
    }

    private NettyConnectionFactory() {

    }

    public NettyClient getConnection(String host, int port) throws InterruptedException {
        String key = host + ":" + port;
        ConcurrentLinkedQueue<NettyClient> pool = pools.get(key);
        if (pool == null) {
            //防止多线程同时操作
            try {
                lock.lock();
                ConcurrentLinkedQueue<NettyClient> pool2 = pools.get(key);
                if (pool2 == null) {
                    pool = new ConcurrentLinkedQueue<NettyClient>();
                    pools.put(key, pool);
                } else
                    pool = pool2;
            } finally {
                lock.unlock();
            }
        }
        NettyClient conn = pool.poll();
        if (conn != null)
            return conn;
        conn = createConnection(host, port);
        return conn;
    }

    public void releaseConnection(NettyClient conn) {
        String key = conn.getHost() + ":" + conn.getPort();
        ConcurrentLinkedQueue<NettyClient> pool = pools.get(key);
        if (pool.size() < AnnularQueue.getInstance().getConfig().getNettyPoolSize()) {
            pool.add(conn);
        } else {
            conn.close();
        }
    }

    private NettyClient createConnection(String host, int port) throws InterruptedException {
        return new NettyClient(host, port);
    }
}
