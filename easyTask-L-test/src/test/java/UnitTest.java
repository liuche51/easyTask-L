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

        TransactionLog log=new TransactionLog();
        log.setTableName("dddd");
        log.setId("qqqqq");
        List<String> list=new LinkedList<>();
        list.add("172.20.50.128:2022");
        list.add("172.20.50.128:2022");
        log.setFollows(JSONObject.toJSONString(list));
        try {
            AnnularQueue.getInstance().getConfig().setTaskStorePath("C:\\db\\node2");
            DbInit.init();
            TransactionLogDao.isExistById("T4ce5cf5c9d0048bda40dd09a7a8f376b-446");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

