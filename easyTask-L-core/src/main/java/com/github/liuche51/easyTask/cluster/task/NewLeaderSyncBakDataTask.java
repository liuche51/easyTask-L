package com.github.liuche51.easyTask.cluster.task;

import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.enume.NodeSyncDataStatusEnum;

import java.util.List;
/**
 * 新leader将旧leader的备份数据同步给自己的follow
 * 后期需要考虑数据一致性
 */
public class NewLeaderSyncBakDataTask extends OnceTask {
    private String oldLeaderAddress;

    public String getOldLeaderAddress() {
        return oldLeaderAddress;
    }

    public void setOldLeaderAddress(String oldLeaderAddress) {
        this.oldLeaderAddress = oldLeaderAddress;
    }

    public NewLeaderSyncBakDataTask(String oldLeaderAddress) {
        this.oldLeaderAddress = oldLeaderAddress;
    }

    @Override
    public void run() {
        try {
            while (!isExit()) {
                List<ScheduleBak> baks = ScheduleBakDao.getBySourceWithCount(oldLeaderAddress, 5);
                if (baks.size() == 0) {//如果已经同步完，标记状态并则跳出循环
                    setExit(true);
                    break;
                }
                baks.forEach(x -> {
                    Task task = Task.valueOf(x);
                    try {
                        AnnularQueue.getInstance().submitForInner(task);//模拟客户端重新提交任务
                        ScheduleBakDao.delete(x.getId());
                    } catch (Exception e) {
                        log.error("submitNewTaskByOldLeader()->", e);
                    }
                });
            }
        } catch (Exception e) {
            log.error("submitNewTaskByOldLeader()->", e);
        }
    }
}
