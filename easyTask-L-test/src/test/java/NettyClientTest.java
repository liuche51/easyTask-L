import com.github.liuche51.easyTask.backup.client.ClientHandler;
import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.junit.Test;

import java.net.InetSocketAddress;

public class NettyClientTest {
    @Test
    public void sendSyncMsg(){
        NettyClient client=new NettyClient(new InetSocketAddress(2020));
        try {
            while (true){
                Thread.sleep(5000);
                ScheduleDto.Schedule.Builder builder=ScheduleDto.Schedule.newBuilder();
                String id=String.valueOf(System.currentTimeMillis());
                builder.setId(id).setClassPath("com.github.liuche51.easyTask.test.task.CusTask1").setExecuteTime(1586078809995l)
                        .setTaskType("PERIOD").setPeriod(30).setUnit("SECONDS")
                        .setParam("birthday#;1986-1-1&;threadid#;1&;name#;Jack&;age#;32&");
                Dto.Frame.Builder builder1=Dto.Frame.newBuilder();
                builder1.setInterfaceName("ScheduleBackup").setClassName("Schedule").setBodyBytes(builder.build().toByteString());
                System.out.println("发送任务:"+id);
                Object msg= client.sendSyncMsg(builder1.build());
                Dto.Frame frame= (Dto.Frame) msg;
                String ret=frame.getBody();
                System.out.println("服务器返回:"+ret);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void sendASyncMsg(){
        NettyClient client=new NettyClient(new InetSocketAddress(2020));
        try {
            while (true){
                Thread.sleep(5000);
                ScheduleDto.Schedule.Builder builder=ScheduleDto.Schedule.newBuilder();
                String id=String.valueOf(System.currentTimeMillis());
                builder.setId(id).setClassPath("com.github.liuche51.easyTask.test.task.CusTask1").setExecuteTime(1586078809995l)
                        .setTaskType("PERIOD").setPeriod(30).setUnit("SECONDS")
                        .setParam("birthday#;1986-1-1&;threadid#;1&;name#;Jack&;age#;32&");
                Dto.Frame.Builder builder1=Dto.Frame.newBuilder();
                builder1.setInterfaceName("ScheduleBackup").setClassName("Schedule").setBodyBytes(builder.build().toByteString());
                System.out.println("发送任务:"+id);
                ChannelFuture future= client.sendASyncMsg(builder1.build());
                future.addListener(new GenericFutureListener<Future<? super Void>>() {
                    @Override
                    public void operationComplete(Future<? super Void> future) throws Exception {
                        if(future.isSuccess()){
                            Object msg=client.getHandler().getResponse();
                            Dto.Frame frame= (Dto.Frame) msg;
                            String ret=frame.getBody();
                            System.out.println("服务器返回:"+ret);
                        }

                    }
                });
                //Dto.Frame frame= (Dto.Frame) msg;
                //String ret=frame.getBody();
                //System.out.println("服务器返回:"+ret);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
