package com.github.liuche51.easyTask.cluster.leader;

import com.alibaba.fastjson.JSONObject;
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
import com.github.liuche51.easyTask.util.DateUtils;
import com.github.liuche51.easyTask.util.StringConstant;
import org.apache.log4j.Logger;

import java.time.ZonedDateTime;
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
            int count= EasyTaskConfig.getInstance().getBackupCount();
            ZKNode node = ZKService.getDataByCurrentNode();
            if(node==null||node.getFollows()==null||node.getFollows().size()<count){//不存在注册信息或者follow节点不足就重新选择follow
                List<String> availableFollows=ZKService.getChildrenByNameSpase();
                //排除自己
                Optional<String> temp=availableFollows.stream().filter(x->x.equals(EasyTaskConfig.getInstance().getzKServerName())).findFirst();
                if(temp.isPresent())
                    availableFollows.remove(temp.get());
                if(availableFollows.size()<count)//如果可选备库节点数量不足，则等待1s，然后重新选。注意：等待会阻塞整个服务可用性
                {
                    log.info("availableFollows is not enough! only has "+availableFollows.size());
                    Thread.sleep(1000);
                    initSelectFollows();
                } else {
                    Random random = new Random();
                    for (int i = 0; i <count; i++) {
                        int index = random.nextInt(availableFollows.size());//随机生成的随机数范围就变成[0,size)。
                        ZKNode node2=ZKService.getDataByPath(StringConstant.CHAR_SPRIT+availableFollows.get(index));
                        log.debug(JSONObject.toJSONString(node));
                        //如果最后心跳时间超过60s，则直接删除该节点信息。
                        if(ZonedDateTime.now().minusSeconds(EasyTaskConfig.getInstance().getDeleteZKTimeOunt())
                        .compareTo(DateUtils.parse(node2.getLastHeartbeat()))>0){
                            ZKService.deleteNodeByPathIgnoreResult(StringConstant.CHAR_SPRIT+availableFollows.get(index));
                        }else if(ZonedDateTime.now().minusSeconds(EasyTaskConfig.getInstance().getSelectLeaderZKNodeTimeOunt())
                                .compareTo(DateUtils.parse(node2.getLastHeartbeat()))>0){
                            //如果最后心跳时间超过30s，也不能将该节点作为follow
                        }else if(follows.size()<count){
                            follows.add(new Node(node2.getHost(),node2.getPort()));
                            if(follows.size()==count)//已选数量够了就跳出
                                break;
                        }
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
