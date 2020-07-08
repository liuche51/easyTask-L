package com.github.liuche51.easyTask.cluster.follow;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.cluster.task.CheckLeadersAliveTask;
import com.github.liuche51.easyTask.cluster.task.TimerTask;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dao.TransactionLogDao;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.TransactionLog;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.enume.TransactionStatusEnum;
import com.github.liuche51.easyTask.enume.TransactionTableEnum;
import com.github.liuche51.easyTask.enume.TransactionTypeEnum;
import com.github.liuche51.easyTask.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Follow服务入口
 */
public class FollowService {
    private static final Logger log = LoggerFactory.getLogger(LeaderService.class);
    /**
     * 接受leader同步任务入备库
     *
     * @param schedule
     */
    public static void trySaveTask(ScheduleDto.Schedule schedule) throws Exception {
        ScheduleBak bak = ScheduleBak.valueOf(schedule);
        TransactionLog transactionLog =new TransactionLog();
        transactionLog.setId(schedule.getTransactionId());
        transactionLog.setContent(JSONObject.toJSONString(bak));
        transactionLog.setStatus(TransactionStatusEnum.TRIED);
        transactionLog.setType(TransactionTypeEnum.SAVE);
        transactionLog.setTableName(TransactionTableEnum.SCHEDULE_BAK);
        TransactionLogDao.save(transactionLog);
    }

    /**
     * 确认提交任务备份
     * @param transactionId
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void confirmSaveTask(String transactionId) throws SQLException, ClassNotFoundException {
        TransactionLogDao.updateStatusById(transactionId,TransactionStatusEnum.CONFIRM);
    }

    /**
     * 取消备份任务
     * @param transactionId
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void cancelSaveTask(String transactionId) throws SQLException, ClassNotFoundException {
        TransactionLogDao.updateStatusById(transactionId,TransactionStatusEnum.CANCEL);
    }
    /**
     * 接受leader同步删除任务
     */
    public static void tryDelTask(String transactionId,String scheduleId) throws Exception {
        TransactionLog transactionLog =new TransactionLog();
        transactionLog.setId(transactionId);
        transactionLog.setContent(scheduleId);
        transactionLog.setStatus(TransactionStatusEnum.TRIED);
        transactionLog.setType(TransactionTypeEnum.DELETE);
        transactionLog.setTableName(TransactionTableEnum.SCHEDULE_BAK);
        TransactionLogDao.save(transactionLog);
    }
    /**
     * 接受leader批量同步任务入备库
     *
     * @param scheduleList
     */
    public static void saveScheduleBakBatch(ScheduleDto.ScheduleList scheduleList) throws Exception {
        List<ScheduleDto.Schedule> list= scheduleList.getSchedulesList();
        if(list==null) return;
        List<ScheduleBak> baklist=new ArrayList<>(list.size());
        list.forEach(x->{
            ScheduleBak bak = ScheduleBak.valueOf(x);
            baklist.add(bak);
        });
        ScheduleBakDao.saveBatch(baklist);
    }
    /**
     * 删除备库任务
     *
     * @param taskId
     */
    public static void deleteScheduleBak(String taskId) throws SQLException, ClassNotFoundException {
        ScheduleBakDao.delete(taskId);
    }

    /**
     * 更新leader位置信息
     *
     * @param leader
     * @return
     */
    public static boolean updateLeaderPosition(String leader) {
        try {
            if (StringUtils.isNullOrEmpty(leader)) return false;
            String[] temp = leader.split(":");
            if (temp.length != 2) return false;
            Map<String, Node> leaders = ClusterService.CURRENTNODE.getLeaders();
            leaders.put(leader, new Node(temp[0], Integer.valueOf(temp[1]).intValue()));
            return true;
        } catch (Exception e) {
            log.error("updateLeaderPosition", e);
            return false;
        }
    }

    /**
     * 节点对zk的心跳。检查leader是否失效。
     * 失效则进入选举
     */
    public static TimerTask initCheckLeaderAlive() {
        CheckLeadersAliveTask task=new CheckLeadersAliveTask();
        task.start();
        return task;
    }

}
