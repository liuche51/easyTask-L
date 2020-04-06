package com.github.liuche51.easyTask.cluster;

import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dto.Schedule;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import io.netty.channel.Channel;

/**
 * Leader Sync Data To Follows
 */
public class LeaderToFollow {
    public static void syncDataToFollow(Schedule schedule){
        for (NettyClient client:ClusterService.FOLLOWS){
            Channel channel=client.getChannelFuture().channel();
            ScheduleDto.Schedule s=schedule.toScheduleDto();
            Dto.Frame.Builder builder=Dto.Frame.newBuilder();
            builder.setClassName("Schedule").setBodyBytes(s.toByteString());
            channel.writeAndFlush(builder);
        }

    }
    public static void saveScheduleBak(ScheduleDto.Schedule dto){
        ScheduleBak bak=ScheduleBak.valueOf(dto);
        ScheduleBakDao.save(bak);
    }
}
