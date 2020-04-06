package com.github.liuche51.easyTask.cluster;

import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleDto;
import io.netty.channel.Channel;

/**
 * Leader Sync Data To Follows
 */
public class LeaderToFollow {
    public static void syncDataToFollow(Schedule schedule){
        for (NettyClient client:ClusterService.FOLLOWS){
            Channel channel=client.getChannelFuture().channel();
            ScheduleDto.Schedule schedule1=schedule.toScheduleDto();
            channel.writeAndFlush(schedule1.toByteArray());
        }

    }
}
