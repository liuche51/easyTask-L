package com.github.liuche51.easyTask.backup.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 *客户端入站(收到服务端消息)事件监听。
 */
public class ClientHandler extends SimpleChannelInboundHandler<Object> {
	 @Override
	    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object msg) {
	    	System.out.println("Server say : " + msg);
	    }
	    @Override
	    public void channelActive(ChannelHandlerContext ctx) throws Exception {
	        System.out.println("Client active ");
	        super.channelActive(ctx);
	    }

	    @Override
	    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
	        System.out.println("Client close ");
	        super.channelInactive(ctx);
	    }
}