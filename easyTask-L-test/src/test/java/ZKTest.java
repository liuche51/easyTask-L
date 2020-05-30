import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dto.zk.ZKFollow;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.register.ZKService;
import com.github.liuche51.easyTask.register.ZKUtil;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;

public class ZKTest {
    public ZKTest() {
        try {

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void register() {
        try {
            ZKNode data = new ZKNode();
            data.setHost("127.0.0.1");
            data.setCreateTime(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            data.setLastHeartbeat(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            List<ZKFollow> follows = new LinkedList<>();
            ZKFollow follow1 = new ZKFollow("127.0.0.2");
            ZKFollow follow2 = new ZKFollow("127.0.0.3");
            follows.add(follow1);
            follows.add(follow2);
            data.setFollows(follows);
            ZKService.register(data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getChildrenByCurrentNode() {
        try {
            List<String> list = ZKService.getChildrenByCurrentNode();
            list.forEach(x -> {
                System.out.println(x);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getChildrenByNameSpase() {
        try {
            List<String> list = ZKService.getChildrenByNameSpase();
            list.forEach(x -> {
                System.out.println(x);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getDataByCurrentNode() {
        try {
            ZKNode node = ZKService.getDataByCurrentNode();
            System.out.println(JSONObject.toJSON(node));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
