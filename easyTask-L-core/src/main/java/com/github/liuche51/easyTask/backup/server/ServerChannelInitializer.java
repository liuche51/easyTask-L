package com.github.liuche51.easyTask.backup.server;

import com.github.liuche51.easyTask.dto.proto.Dto;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

/**
 * <b>标题:    </b><br />
 * <pre>
 * </pre>
 *
 * @author 毛宇鹏
 * @date 创建于 上午9:13 2018/4/25
 */
public class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {
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
        // 配置入站、出站事件channel 
        socketChannel.pipeline().addLast(new ServerHandler());
    }
}