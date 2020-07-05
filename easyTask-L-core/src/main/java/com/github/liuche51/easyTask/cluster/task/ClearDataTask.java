package com.github.liuche51.easyTask.cluster.task;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dao.ScheduleSyncDao;
import com.github.liuche51.easyTask.dao.TransactionDao;
import com.github.liuche51.easyTask.enume.ScheduleSyncStatusEnum;
import com.github.liuche51.easyTask.enume.TransactionStatusEnum;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 清理无用的数据定时任务
 */
public class ClearDataTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                Map<String, Node> leaders = ClusterService.CURRENTNODE.getLeaders();
                Iterator<Map.Entry<String, Node>> items = leaders.entrySet().iterator();//使用遍历+移除操作安全的迭代器方式
                List<String> sources = new ArrayList<>(leaders.size());
                while (items.hasNext()) {
                    Map.Entry<String, Node> item = items.next();
                    sources.add(item.getValue().getAddress());
                }
                ScheduleBakDao.deleteBySources(sources.toArray(new String[sources.size()]));
                TransactionDao.deleteByStatus(TransactionStatusEnum.FINISHED);
                ScheduleSyncDao.deleteByStatus(ScheduleSyncStatusEnum.DELETED);
            } catch (Exception e) {
                log.error("clearScheduleBak()", e);
            }
            try {
                Thread.sleep(EasyTaskConfig.getInstance().getClearScheduleBakTime());
            } catch (InterruptedException e) {
                log.error("clearScheduleBak()", e);
            }
        }
    }
}
