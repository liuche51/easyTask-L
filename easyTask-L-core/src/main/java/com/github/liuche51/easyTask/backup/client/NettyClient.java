package com.github.liuche51.easyTask.backup.client;

import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dto.proto.Dto;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Netty网络连接客户端
 */
public class NettyClient {

    private static final Logger log = Logger.getLogger(NettyClient.class);

    private Bootstrap bootstrap;
    private ChannelFuture channelFuture;
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    ClientHandler handler;
    /**
     * 客户端通道
     */
    private Channel clientChannel;

    public NettyClient(InetSocketAddress address) {
        this.handler = new ClientHandler();
        try {
            log.info("nettyClinet start...");
            bootstrap = new Bootstrap();
            bootstrap.group(workerGroup);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) {
                    // 解码编码
                    // 半包的处理
                    socketChannel.pipeline().addLast(new ProtobufVarint32FrameDecoder());
                    // 需要解码的目标类
                    socketChannel.pipeline().addLast(new ProtobufDecoder(Dto.Frame.getDefaultInstance()));
                    socketChannel.pipeline().addLast(new ProtobufVarint32LengthFieldPrepender());
                    socketChannel.pipeline().addLast(new ProtobufEncoder());
                    // 自己的逻辑Handler
                    socketChannel.pipeline().addLast(handler);
                }
            });
            channelFuture = bootstrap.connect(address).sync();
            clientChannel = channelFuture.channel();
            //注册连接事件
            channelFuture.addListener((ChannelFutureListener) future -> {
                //如果连接成功
                if (future.isSuccess()) {
                    log.info("客户端[" + channelFuture.channel().localAddress().toString() + "]已连接...");
                }
                //如果连接失败，尝试重新连接
                else {
                    log.info("客户端[" + channelFuture.channel().localAddress().toString() + "]连接失败，重新连接中...");
                    future.channel().close();
                    channelFuture = bootstrap.connect(address).sync();
                    clientChannel = channelFuture.channel();
                }
            });

            //注册关闭事件
            channelFuture.channel().closeFuture().addListener(cfl -> {
                close();
                log.info("客户端[" + channelFuture.channel().localAddress().toString() + "]已断开...");
            });
            log.info("nettyClinet started...");
        } catch (Exception e) {
            log.error("nettyClinet started fail.", e);
        }
    }

    public Channel getClientChannel() {
        return clientChannel;
    }

    /**
     * 发送同步消息
     *
     * @param msg
     * @return
     */
    public Object sendSyncMsg(Object msg) throws InterruptedException {
        ChannelPromise promise = clientChannel.newPromise();
        handler.setPromise(promise);
        clientChannel.writeAndFlush(msg);
        promise.await(EasyTaskConfig.getInstance().getTimeOut(), TimeUnit.SECONDS);
        return handler.getResponse();
    }

    /**
     * 发送异步消息
     *
     * @param msg
     * @return
     */
    public ChannelFuture sendASyncMsg(Object msg) {
        return clientChannel.writeAndFlush(msg);
    }

    /**
     * 客户端关闭
     */
    private void close() {
        //关闭客户端套接字
        if (clientChannel != null) {
            clientChannel.close();
        }
        //关闭客户端线程组
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
    }
}