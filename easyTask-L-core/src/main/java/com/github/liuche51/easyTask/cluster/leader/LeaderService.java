package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.backup.server.NettyServer;
import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.dto.zk.ZKNode;
import com.github.liuche51.easyTask.register.ZKService;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * Leader服务入口
 */
public class LeaderService {
    private static final Logger log = Logger.getLogger(LeaderService.class);
    /**
     * 节点启动初始化。
     * 先查看自己是否在zk注册，没有注册则直接选择follow。注册了就继续使用当前的follow
     * @return
     */
    public static boolean initSelectFollows() {
        try {
            List<Node> follows = new LinkedList<>();//备选follows
            ZKNode node = ZKService.getDataByCurrentNode();
            if(node==null){//不存在注册信息就重新选择follow
                List<String> availableFollows=ZKService.getChildrenByNameSpase();
                //排除自己
                Optional<String> temp=availableFollows.stream().filter(x->x.equals(EasyTaskConfig.getInstance().getzKServerName())).findFirst();
                if(temp.isPresent())
                    availableFollows.remove(temp.get());
                int count= EasyTaskConfig.getInstance().getBackupCount();
                if(availableFollows.size()<count)//如果可选备库节点数量不足，则等待1s，然后重新选。注意：等待会阻塞整个服务可用性
                {
                    log.info("availableFollows is not enough! only has "+availableFollows.size());
                    Thread.sleep(1000);
                    initSelectFollows();
                }
                if (availableFollows.size() == 0) {
                }
                else if (availableFollows.size() == 1&&count>0) {
                    ZKNode node1=ZKService.getDataByPath(availableFollows.get(0));
                    follows.add(new Node(node1.getHost(),node1.getPort()));
                } else {
                    Random random = new Random();
                    for (int i = 0; i <count; i++) {
                        int index = random.nextInt(availableFollows.size() + 1);
                        ZKNode node2=ZKService.getDataByPath(availableFollows.get(index));
                        follows.add(new Node(node2.getHost(),node2.getPort()));
                        availableFollows.remove(index);
                    }
                }
            }else if(node.getFollows()!=null){
                node.getFollows().forEach(x->{
                    follows.add(new Node(x.getHost(),x.getPort()));
                });
            }
            ClusterService.CURRENTNODE.setFollows(follows);
        }catch (Exception e){
            log.error("node select follows error.",e);
        }
        return true;
    }

    /**
     * 同步任务至follow。
     *
     * @param schedule
     * @return
     */
    public static boolean syncDataToFollow(Schedule schedule, Node node) {
        for (Node follow : node.getFollows()) {
            ScheduleDto.Schedule s = schedule.toScheduleDto();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setInterfaceName("ScheduleBackup").setClassName("Schedule").setBodyBytes(s.toByteString());
            NettyClient client = follow.getClient();
            boolean ret = LeaderUtil.sendSyncMsgWithCount(client, builder.build(), 3);
            if (ret) continue;
            else return false;
        }
        return true;
    }
}
