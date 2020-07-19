import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dao.DbInit;
import com.github.liuche51.easyTask.dao.TransactionLogDao;
import com.github.liuche51.easyTask.dto.TransactionLog;
import com.github.liuche51.easyTask.enume.TransactionStatusEnum;
import com.github.liuche51.easyTask.monitor.ClusterMonitor;
import com.github.liuche51.easyTask.util.DateUtils;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class UnitTest {
    public static List<Thread> threadList=new LinkedList<>();
    @Test
    public void test()  {
        try {
            EasyTaskConfig config=new EasyTaskConfig();
            config.setTaskStorePath("C:/db/node1");
            //AnnularQueue.getInstance().setConfig(config);
           ClusterMonitor.getDBTraceInfoByTaskId("229d631f1d264a8a922d0c4c5f752b45-19");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

