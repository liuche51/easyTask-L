package com.github.liuche51.easyTask.netty.server;

import com.alibaba.fastjson.JSONObject;
import com.github.liuche51.easyTask.cluster.follow.FollowService;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.monitor.DBMonitor;
import com.github.liuche51.easyTask.dto.proto.ResultDto;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTask.util.DateUtils;
import com.github.liuche51.easyTask.util.StringConstant;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

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
        try {
            builder.setSource(AnnularQueue.getInstance().getConfig().getAddress());
            Dto.Frame frame = (Dto.Frame) msg;
            builder.setIdentity(frame.getIdentity());
            switch (frame.getInterfaceName()) {
                case NettyInterfaceEnum
                        .TRAN_TRYSAVETASK:
                    ScheduleDto.Schedule schedule = ScheduleDto.Schedule.parseFrom(frame.getBodyBytes());
                    FollowService.trySaveTask(schedule);
                    break;
                case NettyInterfaceEnum
                        .TRAN_CONFIRMSAVETASK:
                    String transactionId = frame.getBody();
                    FollowService.confirmSaveTask(transactionId);
                    break;
                case NettyInterfaceEnum
                        .TRAN_CANCELSAVETASK:
                    String transactionId1 = frame.getBody();
                    FollowService.cancelSaveTask(transactionId1);
                    break;
                case NettyInterfaceEnum
                        .TRAN_TRYDELTASK:
                    String tranAndSchedule = frame.getBody();
                    String[] item=tranAndSchedule.split(",");
                    FollowService.tryDelTask(item[0],item[1]);
                    break;
                case NettyInterfaceEnum
                        .LEADER_SYNC_DATA_TO_NEW_FOLLOW:
                    ScheduleDto.ScheduleList scheduleList = ScheduleDto.ScheduleList.parseFrom(frame.getBodyBytes());
                    FollowService.saveScheduleBakBatchByTran(scheduleList);
                    break;
                case NettyInterfaceEnum.SYNC_LEADER_POSITION:
                    String ret = frame.getBody();
                    FollowService.updateLeaderPosition(ret);
                    break;
                case NettyInterfaceEnum.GET_DBINFO_BY_TASKID:
                    String tranId = frame.getBody();
                    Map<String, List> map=DBMonitor.getInfoByTaskId(tranId);
                    result.setBody(JSONObject.toJSONString(map));
                    break;
                case NettyInterfaceEnum.SYNC_CLOCK_DIFFER:
                    result.setBody(String.valueOf(System.currentTimeMillis()));
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