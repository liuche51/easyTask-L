package com.github.liuche51.easyTask.backup.server;

import com.github.liuche51.easyTask.cluster.LeaderToFollow;
import com.github.liuche51.easyTask.cluster.follow.FollowService;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.google.protobuf.InvalidProtocolBufferException;
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
		builder.setInterfaceName("Result").setClassName("Common");
		try {
			Dto.Frame frame= (Dto.Frame) msg;
			switch (frame.getInterfaceName()){
				case "ScheduleBackup":
					switch (frame.getClassName()){
						case "Schedule":
							ScheduleDto.Schedule schedule=ScheduleDto.Schedule.parseFrom(frame.getBodyBytes());
							FollowService.saveScheduleBak(schedule);
							int y=0;
							break;

					}
				case "Result":
					switch (frame.getClassName()){
						case "Common":
							String ret=frame.getBody();

					}
			}
		} catch (Exception e) {
			log.error("Deal client msg occured error！",e);
			builder.setBody("false");
		}
		builder.setBody("true");
		// 返回客户端消息 - 我已经接收到了你的消息
		ctx.writeAndFlush(builder);
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