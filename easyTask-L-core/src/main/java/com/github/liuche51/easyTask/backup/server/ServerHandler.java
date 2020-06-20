package com.github.liuche51.easyTask.backup.server;

import com.github.liuche51.easyTask.cluster.follow.FollowService;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dto.proto.ResultDto;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.util.StringConstant;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

/**
 *
 */
public class ServerHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger log = LoggerFactory.getLogger(ServerHandler.class);

    /**
     * 接受客户端发过来的消息。
     *
     * @param ctx
     * @param msg
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        // 收到消息直接打印输出
		StringBuilder str=new StringBuilder("Received Client:");
		str.append(ctx.channel().remoteAddress()).append( " send : ").append(msg);
        log.debug(str.toString());
        Dto.Frame.Builder builder = Dto.Frame.newBuilder();
        ResultDto.Result.Builder result = ResultDto.Result.newBuilder();
        result.setResult(StringConstant.TRUE);
        builder.setInterfaceName(StringConstant.TRUE);
        builder.setSource(EasyTaskConfig.getInstance().getzKServerName());
        try {
            Dto.Frame frame = (Dto.Frame) msg;
            builder.setIdentity(frame.getIdentity());
            switch (frame.getInterfaceName()) {
                case StringConstant
                        .SYNC_SCHEDULE_BACKUP:
                    ScheduleDto.Schedule schedule = ScheduleDto.Schedule.parseFrom(frame.getBodyBytes());
                    FollowService.saveScheduleBak(schedule);
                    break;
                case StringConstant.SYNC_LEADER_POSITION:
                    String ret = frame.getBody();
                    FollowService.updateLeaderPosition(ret);
                    break;
                case StringConstant.DELETE_SCHEDULEBACKUP:
                    String taskId = frame.getBody();
                    FollowService.deleteScheduleBak(taskId);
                    break;
                default:
                    throw new Exception("unknown interface method");
            }
        } catch (Exception e) {
            log.error("Deal client msg occured error！", e);
            result.setResult(StringConstant.FALSE);
        }
        builder.setBodyBytes(result.build().toByteString());
        ctx.writeAndFlush(builder.build());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        System.out.println("RamoteAddress : " + ctx.channel().remoteAddress() + " active !");
        try {
            ctx.writeAndFlush("Welcome to " + InetAddress.getLocalHost().getHostName() + " service!\n");
            super.channelActive(ctx);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            log.error("channelActive exception!", e);
        }

    }
}