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
 * Netty网络连接客户端
 */
public class NettyClient {

    private static final Logger log = LoggerFactory.getLogger(NettyClient.class);
    private InetSocketAddress address;
    private Bootstrap bootstrap;
    private ChannelFuture channelFuture;
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    private ClientHandler handler;

    public ClientHandler getHandler() {
        return handler;
    }

    /**
     * 客户端通道
     */
    private Channel clientChannel;

    public NettyClient(InetSocketAddress address) {
        this.handler = new ClientHandler();
        this.address=address;
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
                future.get();
                //如果连接成功
                if (future.isSuccess()) {
                    log.info("Client[" + channelFuture.channel().localAddress().toString() + "]connected...");
                }
                //如果连接失败，尝试重新连接
                else {
                    log.info("Client[" + channelFuture.channel().localAddress().toString() + "]connect failed，重新连接中...");
                    future.channel().close();
                    channelFuture = bootstrap.connect(address).sync();
                    clientChannel = channelFuture.channel();
                }
            });

            //注册关闭事件
            channelFuture.channel().closeFuture().addListener(cfl -> {
                close();
                log.info("Client[" + channelFuture.channel().localAddress().toString() + "]disconnected...");
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
    public Object sendSyncMsg(Object msg) throws InterruptedException, ConnectionException {
        sendMsgPrintLog(msg);
        if(clientChannel==null) throw new ConnectionException("sendSyncMsg->"+this.address+": object node has disconnected!");
        ChannelPromise promise = clientChannel.newPromise();
        handler.setPromise(promise);
        clientChannel.writeAndFlush(msg);
        promise.await(AnnularQueue.getInstance().getConfig().getTimeOut(), TimeUnit.SECONDS);//等待固定的时间，超时就认为失败，需要重发
        return handler.getResponse();
    }

    private void sendMsgPrintLog(Object msg) {
        StringBuilder str=new StringBuilder("Client send to:");
        str.append(getObjectAddress()).append( " msg : ").append(msg);
        log.debug(str.toString());
    }

    /**
     * 获取目标连接主机地址
     * @return
     */
    public String getObjectAddress() {
        return address.getHostName()+":"+address.getPort();
    }

    /**
     * 发送异步消息
     *
     * @param msg
     * @return
     */
    public ChannelFuture sendASyncMsg(Object msg) throws ConnectionException {
        sendMsgPrintLog(msg);
        if(clientChannel==null) throw new ConnectionException("sendASyncMsg->"+this.address+": object node has disconnected!");
        ChannelPromise promise = clientChannel.newPromise();
        handler.setPromise(promise);
        return clientChannel.writeAndFlush(msg);
    }
    /**
     * 发送异步消息。不通过信号量控制。
     * 可以实现一个Nettyclient并发处理N个请求。但不能使用future.addListener方式处理返回结果了。
     *需要在com.github.liuche51.easyTask.backup.client.ClientHandler#channelRead0中统一处理。
     * 这样需要每个请求中附带一个唯一标识。服务端返回结果时也戴上这个标识才行。否则就不知道处理的是哪个请求返回的结果。
     * @param msg
     * @return
     */
    public ChannelFuture sendASyncMsgWithoutPromise(Object msg) throws ConnectionException {
        sendMsgPrintLog(msg);
        if(clientChannel==null) throw new ConnectionException("sendASyncMsg->"+this.address+": object node has disconnected!");
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