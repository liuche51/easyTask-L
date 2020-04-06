package com.github.liuche51.easyTask.backup.server;

import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.net.InetAddress;

/**
 *
 */
public class ServerHandler extends SimpleChannelInboundHandler<Object> {

	@Override
	public void channelRead0(ChannelHandlerContext ctx, Object msg) {
		try {
			Dto.Frame frame= (Dto.Frame) msg;
			switch (frame.getInterfaceName()){
				case "ScheduleBackup":
					switch (frame.getClassName()){
						case "Schedule":
							ScheduleDto.Schedule schedule=ScheduleDto.Schedule.parseFrom(frame.getBodyBytes());
							int y=0;
							break;
					}
			}


		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		// 收到消息直接打印输出
        System.out.println(ctx.channel().remoteAddress() + " Say : " + msg);
        // 返回客户端消息 - 我已经接收到了你的消息
        ctx.writeAndFlush("Received your message !\n");
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