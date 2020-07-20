package com.github.liuche51.easyTask.cluster.task;

import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.leader.LeaderUtil;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dao.ScheduleSyncDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleSync;
import com.github.liuche51.easyTask.enume.NodeSyncDataStatusEnum;
import com.github.liuche51.easyTask.enume.ScheduleSyncStatusEnum;
import com.github.liuche51.easyTask.util.exception.VotedException;
import com.github.liuche51.easyTask.util.exception.VotingException;

import java.util.List;
/**
 * leader同步数据到新follow
 * 目前设计为只有一个线程同步给某个follow
 */
public class SyncDataToNewFollowTask extends OnceTask {
    private Node oldFollow;
    private Node newFollow;

    public Node getOldFollow() {
        return oldFollow;
    }

    public void setOldFollow(Node oldFollow) {
        this.oldFollow = oldFollow;
    }

    public Node getNewFollow() {
        return newFollow;
    }

    public void setNewFollow(Node newFollow) {
        this.newFollow = newFollow;
    }
    public SyncDataToNewFollowTask(Node oldFollow, Node newFollow){
        this.oldFollow=oldFollow;
        this.newFollow=newFollow;
    }
    @Override
    public void run() {
        try {
            //先将失效的follow数据同步标记为未同步，同时修改其follow标识
            ScheduleSyncDao.updateFollowAndStatusByFollow(oldFollow.getAddress(), newFollow.getAddress(), ScheduleSyncStatusEnum.UNSYNC);
            while (!isExit()) {
                //获取批次数据
                List<ScheduleSync> list = ScheduleSyncDao.selectByFollowAndStatusWithCount(newFollow.getAddress(), ScheduleSyncStatusEnum.UNSYNC, 5);
                if (list.size() == 0) {//如果已经同步完，标记状态并则跳出循环
                    newFollow.setDataStatus(NodeSyncDataStatusEnum.SYNC);
                    setExit(true);
                    break;
                }
                String[] ids = list.stream().distinct().map(ScheduleSync::getScheduleId).toArray(String[]::new);
                ScheduleSyncDao.updateStatusByFollowAndScheduleIds(newFollow.getAddress(), ids, ScheduleSyncStatusEnum.SYNCING);
                List<Schedule> list1 = ScheduleDao.selectByIds(ids);
                boolean ret = LeaderUtil.syncDataToFollowBatch(list1, newFollow);
                if (ret)
                    ScheduleSyncDao.updateStatusByFollowAndStatus(newFollow.getAddress(), ScheduleSyncStatusEnum.SYNCING, ScheduleSyncStatusEnum.SYNCED);
            }
        } catch (VotingException e) {
            //同步数据异常，进入选举新follow。但此时刚好有其他地方触发正在选举中。当前新follow可能又失效了。
            //此时就没必要继续同步数据给当前新follow了。终止同步线程
            log.error("normally exception error.can ignore."+e.getMessage());
        } catch (VotedException e) {
            //原因同上VotingException
            log.error("normally exception error.can ignore."+e.getMessage());
        } catch (Exception e) {
            log.error("syncDataToNewFollow() exception!", e);
        }
    }
}
