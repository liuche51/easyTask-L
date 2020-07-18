import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.cluster.leader.LeaderService;
import com.github.liuche51.easyTask.cluster.leader.LeaderUtil;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.util.Util;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class LeaderServiceTest {
    @Test
    public void initSelectFollows() {
        try {
            AnnularQueue.getInstance().getConfig().setBackupCount(2);
            ClusterService.CURRENTNODE=new Node("127.0.0.1",2020);
            LeaderService.initSelectFollows();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    @Test
    public void notifyFollowsLeaderPosition(){
        try {
        List<Node> list=new LinkedList<>();
        Node node1=new Node(Util.getLocalIP(),2021);
        list.add(node1);
        LeaderUtil.notifyFollowsLeaderPosition(list,3);
            while (true){
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
