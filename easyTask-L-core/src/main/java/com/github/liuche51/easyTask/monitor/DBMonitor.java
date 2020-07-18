package com.github.liuche51.easyTask.monitor;

import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dao.ScheduleDao;
import com.github.liuche51.easyTask.dao.ScheduleSyncDao;
import com.github.liuche51.easyTask.dao.TransactionLogDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.ScheduleSync;
import com.github.liuche51.easyTask.dto.TransactionLog;
import com.github.liuche51.easyTask.util.StringConstant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBMonitor {
    public static Map<String,List> getInfoByTransactionId(String tranId) throws Exception {
        Map<String,List> map=new HashMap<>(3);
        List<TransactionLog> tranlogs= TransactionLogDao.selectByTransactionId(tranId);
        List<Schedule> schedules= ScheduleDao.selectByTransactionId(tranId);
        List<ScheduleSync> scheduleSyncs= ScheduleSyncDao.selectByTransactionId(tranId);
        List<ScheduleBak> scheduleBaks= ScheduleBakDao.selectByTransactionId(tranId);
        map.put(StringConstant.TRANSACTION_LOG,tranlogs);
        map.put(StringConstant.SCHEDULE,schedules);
        map.put(StringConstant.SCHEDULE_SYNC,scheduleSyncs);
        map.put(StringConstant.SCHEDULE_BAK,scheduleBaks);
        return map;
    }
}
