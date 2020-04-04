package com.github.liuche51.easyTask.backup.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;

/**
 *
 */
public class NettyServer {

	private static final Logger log = Logger.getLogger(NettyServer.class);
	private static NettyServer singleton = null;
	private final EventLoopGroup bossGroup = new NioEventLoopGroup(); // 用来接收进来的连接
	private final EventLoopGroup workerGroup = new NioEventLoopGroup();// 用来处理已经被接收的连接
	public static int port=2020;//本机服务端口
	private Channel channel;
	public static NettyServer getInstance() {
		if (singleton == null) {
			synchronized (NettyServer.class) {
				if (singleton == null) {
					singleton = new NettyServer();
				}
			}
		}
		return singleton;
	}
	private NettyServer() {
	}
	/**
	 * 启动服务
	 */
	public ChannelFuture run() {
		InetSocketAddress address = new InetSocketAddress(port);
		ChannelFuture f = null;
		try {
			ServerBootstrap b = new ServerBootstrap();
			 // 这里告诉Channel如何接收新的连接
			b.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class).childHandler(new ServerChannelInitializer()).option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true);
			// 绑定端口，开始接收进来的连接
			f = b.bind(address).syncUninterruptibly();
			channel = f.channel(); // 等待服务器socket关闭
		} catch (Exception e) {
			log.error("Netty start error:", e);
		} finally {
			if (f != null && f.isSuccess()) {
				log.info("Netty server listening " + address.getHostName() + " on port " + address.getPort() + " and ready for connections...");
			} else {
				log.error("Netty server start up Error!");
			}
		}

		return f;
	}

	public void destroy() {
		log.info("Shutdown Netty Server...");
		if (channel != null) {
			channel.close();
		}
		workerGroup.shutdownGracefully();
		bossGroup.shutdownGracefully();
		log.info("Shutdown Netty Server Success!");
	}
}
