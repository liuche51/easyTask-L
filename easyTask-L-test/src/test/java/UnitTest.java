import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.dto.proto.Dto;
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
                ScheduleDto.Schedule.Builder builder=ScheduleDto.Schedule.newBuilder();
                builder.setId("sdfertert").setClassPath("com.github.liuche51.easyTask.test.task.CusTask1").setExecuteTime(1586078809995l)
                        .setTaskType("PERIOD").setPeriod(30).setUnit("SECONDS")
                        .setParam("birthday#;1986-1-1&;threadid#;1&;name#;Jack&;age#;32&");
                Dto.Frame.Builder builder1=Dto.Frame.newBuilder();
                builder1.setClassName("Schedule").setBodyBytes(builder.build().toByteString());
                channel.writeAndFlush(builder1.build());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
