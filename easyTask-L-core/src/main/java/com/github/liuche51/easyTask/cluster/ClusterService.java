package com.github.liuche51.easyTask.cluster;

import com.github.liuche51.easyTask.backup.client.NettyClient;

import java.util.LinkedList;
import java.util.List;

public class ClusterService {
    /**
     * 数据备份的跟随者
     */
    public static List<NettyClient> FOLLOWS=new LinkedList<>();
}
