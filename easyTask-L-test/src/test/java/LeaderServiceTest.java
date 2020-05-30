import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.core.Util;
import org.junit.Test;

public class LeaderServiceTest {
    @Test
    public void initSelectFollows() {
        try {
            EasyTaskConfig.getInstance().setBackupCount(2);
            ClusterService.CURRENTNODE=new Node("127.0.0.1",2020);
            LeaderService.initSelectFollows();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
