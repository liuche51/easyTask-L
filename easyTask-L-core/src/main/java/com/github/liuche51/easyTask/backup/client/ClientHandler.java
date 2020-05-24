package com.github.liuche51.easyTask.backup.client;

import com.github.liuche51.easyTask.backup.server.ServerHandler;
import com.github.liuche51.easyTask.cluster.follow.FollowService;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

/**
 * 客户端入站(收到服务端消息)事件监听。
 */
public class ClientHandler extends SimpleChannelInboundHandler<Object> {
    protected static final Logger log = Logger.getLogger(ClientHandler.class);
    private ChannelHandlerContext ctx;
    /**
     * 线程同步信号量。用于客户端同步调用服务端
     */
    private ChannelPromise promise;
    /**
     * 同步调用时。返回服务端结果
     */
	private Object response;
    public void setPromise(ChannelPromise promise){
        this.promise=promise;
    }

    public Object getResponse() {
        return response;
    }
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        // 收到消息直接打印输出
        log.debug("Received Server:" + ctx.channel().remoteAddress() + " send : " + msg);
        if (promise != null)
		{
			promise.setSuccess();
			this.response=msg;
		}
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Client active ");
        super.channelActive(ctx);
        this.ctx = ctx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("Client close ");
        super.channelInactive(ctx);
    }

}