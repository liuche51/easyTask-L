import com.github.liuche51.easyTask.backup.client.NettyClient;
import io.netty.channel.Channel;
import org.junit.Test;

import java.net.InetSocketAddress;

public class UnitTest {
    @Test
    public void nettyClient(){
        NettyClient client=new NettyClient(new InetSocketAddress(2020));
        try {
            Channel channel =client.getChannelFuture().channel();
            while (true){
                Thread.sleep(2000);
                channel.writeAndFlush("ddddd");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
