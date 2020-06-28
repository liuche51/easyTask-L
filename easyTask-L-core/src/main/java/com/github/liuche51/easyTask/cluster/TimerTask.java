package com.github.liuche51.easyTask.cluster;

import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TimerTask {
    private static final Logger log = LoggerFactory.getLogger(TimerTask.class);

    /**
     * 清理任务备份表中失效的leader备份
     * 选举新的leader，会主动清理一次。这里防止异常情况遗留下来未清理掉的数据。
     * 建议频率不要太高
     */
    public static Thread clearScheduleBak() {
        Thread th1 = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Map<String, Node> leaders = ClusterService.CURRENTNODE.getLeaders();
                        Iterator<Map.Entry<String, Node>> items = leaders.entrySet().iterator();//使用遍历+移除操作安全的迭代器方式
                        List<String> sources = new ArrayList<>(leaders.size());
                        while (items.hasNext()) {
                            Map.Entry<String, Node> item = items.next();
                            sources.add(item.getValue().getAddress());
                        }
                        ScheduleBakDao.deleteBySources(sources.toArray(new String[sources.size()]));
                    } catch (Exception e) {
                        log.error("clearScheduleBak()", e);
                    }
                    try {
                        Thread.sleep(EasyTaskConfig.getInstance().getClearScheduleBakTime()*60*60*1000);
                    } catch (InterruptedException e) {
                        log.error("clearScheduleBak()",e);
                    }
                }
            }
        });
        th1.start();
        return th1;
    }
}
