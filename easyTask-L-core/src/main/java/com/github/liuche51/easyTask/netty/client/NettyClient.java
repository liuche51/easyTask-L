package com.github.liuche51.easyTask.netty.client;

import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.util.exception.ConnectionException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Netty网络连接客户端。
 */
public class NettyClient {

    private static final Logger log = LoggerFactory.getLogger(NettyClient.class);
    private String host;
    private int port =0;
    private Bootstrap bootstrap;
    private ChannelFuture channelFuture;
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    private ClientHandler handler;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public ClientHandler getHandler() {
        return handler;
    }
    public Channel getClientChannel() {
        return clientChannel;
    }
    /**
     * 客户端通道
     */
    private Channel clientChannel;

    public NettyClient(String host, int port) throws InterruptedException {
        this.host=host;
        this.port=port;
        this.handler = new ClientHandler();
        log.info("nettyClinet start to " + getObjectAddress() + "...");
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
        channelFuture = bootstrap.connect(new InetSocketAddress(host,port)).sync();//sync表示同步阻塞，直到连接成功。
        clientChannel = channelFuture.channel();
        //注册异步连接事件
        channelFuture.addListener((ChannelFutureListener) future -> {
            //如果连接成功
            if (future.isSuccess()) {
                log.info("Client[" + channelFuture.channel().localAddress().toString() + "]connected...");
            }
            //如果连接失败，尝试重新连接
            else {
                log.info("Client[" + channelFuture.channel().localAddress().toString() + "]connect failed，重新连接中...");
                future.channel().close();
                channelFuture = bootstrap.connect(new InetSocketAddress(host,port));//.sync();
                clientChannel = channelFuture.channel();
            }
        });

        //注册关闭事件
        channelFuture.channel().closeFuture().addListener(cfl -> {
            close();
            log.info("Client[" + channelFuture.channel().localAddress().toString() + "]disconnected...");
        });
        log.info("nettyClinet started to " + getObjectAddress() + "...");
    }

    /**
     * 获取目标连接主机地址
     *
     * @return
     */
    public String getObjectAddress() {
        return host + ":" + port;
    }

    /**
     * 客户端关闭
     */
    public void close() {
        //关闭客户端套接字
        if (clientChannel != null) {
            clientChannel.close();
        }
        //关闭客户端线程组
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        log.debug("close()->:客户端连接已关闭");
    }
}