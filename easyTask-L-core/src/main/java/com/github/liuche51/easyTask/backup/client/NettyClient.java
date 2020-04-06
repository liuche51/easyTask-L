package com.github.liuche51.easyTask.backup.client;

import com.github.liuche51.easyTask.dto.ScheduleDto;
import com.github.liuche51.easyTask.dto.proto.Dto;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.Scanner;

/**
 *Netty网络连接客户端
 */
public class NettyClient {

	private static final Logger log = Logger.getLogger(NettyClient.class);

	private Bootstrap bootstrap;
	private ChannelFuture channelFuture;
	private final EventLoopGroup workerGroup = new NioEventLoopGroup();

	public ChannelFuture getChannelFuture() {
		return channelFuture;
	}

	public NettyClient(InetSocketAddress address) {
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
					socketChannel.pipeline().addLast(new ClientHandler());
				}
			});
			channelFuture = bootstrap.connect(address).sync();
			log.info("nettyClinet started...");
		} catch (Exception e) {
			log.error("nettyClinet started fail.",e);
		}
	}
}