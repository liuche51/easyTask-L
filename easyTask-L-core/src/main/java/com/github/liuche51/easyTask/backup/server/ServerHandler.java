package com.github.liuche51.easyTask.backup.server;

import com.github.liuche51.easyTask.cluster.follow.FollowService;
import com.github.liuche51.easyTask.dto.proto.ResultDto;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.util.StringConstant;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

import java.net.InetAddress;

/**
 *
 */
public class ServerHandler extends SimpleChannelInboundHandler<Object> {
	private static final Logger log = Logger.getLogger(ServerHandler.class);
	/**
	 * 接受客户端发过来的消息。
	 * @param ctx
	 * @param msg
	 */
	@Override
	public void channelRead0(ChannelHandlerContext ctx, Object msg) {
		// 收到消息直接打印输出
		log.debug("Received Client:"+ctx.channel().remoteAddress() + " send : " + msg);
		Dto.Frame.Builder builder=Dto.Frame.newBuilder();
		ResultDto.Result.Builder result=ResultDto.Result.newBuilder();
		result.setMsg("success");
		result.setResult("true");
		builder.setBodyBytes(result.build().toByteString());
		builder.setInterfaceName("Result");
		try {
			Dto.Frame frame= (Dto.Frame) msg;
			builder.setIdentity(frame.getIdentity());
			switch (frame.getInterfaceName()){
				case StringConstant
						.SYNC_SCHEDULE_BACKUP:
							ScheduleDto.Schedule schedule=ScheduleDto.Schedule.parseFrom(frame.getBodyBytes());
							FollowService.saveScheduleBak(schedule);
							int y=0;
							break;
				case StringConstant.SYNC_LEADER_POSITION:
							String ret=frame.getBody();
							FollowService.updateLeaderPosition(ret);
							break;
				default:throw new Exception("unknown interface method");
			}
		} catch (Exception e) {
			log.error("Deal client msg occured error！",e);
			builder.setBody("false");
		}

		// 返回客户端消息 - 我已经接收到了你的消息
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
			e.printStackTrace();
		}
		
	}
}