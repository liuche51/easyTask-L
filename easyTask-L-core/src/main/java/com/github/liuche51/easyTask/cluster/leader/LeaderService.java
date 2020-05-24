package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.util.StringUtils;
import io.netty.channel.Channel;
import io.netty.util.internal.StringUtil;

/**
 * Leader服务入口
 */
public class LeaderService {
    public static boolean initSelectFollows(){


    }
    /**
     * 同步任务至follow。
     * @param schedule
     * @return
     */
    public static boolean syncDataToFollow(Schedule schedule, Node node) {
        for (Node follow : node.getFollows()) {
            ScheduleDto.Schedule s = schedule.toScheduleDto();
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setInterfaceName("ScheduleBackup").setClassName("Schedule").setBodyBytes(s.toByteString());
            NettyClient client=follow.getClient();
            boolean ret = LeaderUtil.sendSyncMsgWithCount(client, builder.build(), 3);
            if (ret) continue;
            else return false;
        }
        return true;
    }
}
