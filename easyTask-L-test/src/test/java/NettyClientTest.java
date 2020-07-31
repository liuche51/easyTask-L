import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.netty.client.NettyClient;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
import com.github.liuche51.easyTask.enume.NettyInterfaceEnum;
import com.github.liuche51.easyTask.netty.client.NettyConnectionFactory;
import com.github.liuche51.easyTask.netty.client.NettyMsgService;
import com.github.liuche51.easyTask.util.StringConstant;
import com.github.liuche51.easyTask.util.Util;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.junit.Test;

import java.net.InetSocketAddress;

public class NettyClientTest {
    @Test
    public void sendSyncMsg() throws Exception {
        EasyTaskConfig config=new EasyTaskConfig();
        config.setTimeOut(30);
        config.setNettyPoolSize(2);
        //AnnularQueue.getInstance().setConfig(config);
        try {
            while (true){
                ScheduleDto.Schedule.Builder builder=ScheduleDto.Schedule.newBuilder();
                String id=String.valueOf(System.currentTimeMillis());
                builder.setId(id).setClassPath("com.github.liuche51.easyTask.test.task.CusTask1").setExecuteTime(1586078809995l)
                        .setTaskType("PERIOD").setPeriod(30).setUnit("SECONDS").setSource("127.0.0.1:2020").setTransactionId(Util.generateTransactionId())
                        .setParam("birthday#;1986-1-1&;threadid#;1&;name#;Jack&;age#;32&");
                Dto.Frame.Builder builder1=Dto.Frame.newBuilder();
                builder1.setIdentity(Util.generateIdentityId());
                builder1.setInterfaceName(NettyInterfaceEnum.TRAN_TRYSAVETASK).setBodyBytes(builder.build().toByteString());
                System.out.println("发送任务:"+id);
                NettyClient client=NettyConnectionFactory.getInstance().getConnection("127.0.0.1",2021);
                Object msg= NettyMsgService.sendSyncMsg(client,builder1.build());
                Dto.Frame frame= (Dto.Frame) msg;
                String ret=frame.getBody();
                System.out.println("服务器返回:"+ret);
                Thread.sleep(500000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void sendSyncMsgWithList(){
        try {
            while (true){
                ScheduleDto.Schedule.Builder builder0=ScheduleDto.Schedule.newBuilder();
                String id=String.valueOf(System.currentTimeMillis());
                builder0.setId(id).setClassPath("com.github.liuche51.easyTask.test.task.CusTask1").setExecuteTime(1586078809995l)
                        .setTaskType("PERIOD").setPeriod(30).setUnit("SECONDS").setSource("127.0.0.1:2020").setTransactionId(Util.generateTransactionId())
                        .setParam("birthday#;1986-1-1&;threadid#;1&;name#;Jack&;age#;32&");
                ScheduleDto.Schedule.Builder builder1=ScheduleDto.Schedule.newBuilder();
                String id1=String.valueOf(System.currentTimeMillis()+1);
                builder1.setId(id1).setClassPath("com.github.liuche51.easyTask.test.task.CusTask1").setExecuteTime(1586078809995l)
                        .setTaskType("PERIOD").setPeriod(30).setUnit("SECONDS").setSource("127.0.0.1:2020").setTransactionId(Util.generateTransactionId())
                        .setParam("birthday#;1986-1-1&;threadid#;1&;name#;Jack&;age#;32&");
                ScheduleDto.ScheduleList.Builder builder3=ScheduleDto.ScheduleList.newBuilder();
                builder3.addSchedules(builder0.build());
                builder3.addSchedules(builder1.build());
                Dto.Frame.Builder builder4=Dto.Frame.newBuilder();
                builder4.setIdentity(StringConstant.EMPTY);
                builder4.setInterfaceName(NettyInterfaceEnum.LEADER_SYNC_DATA_TO_NEW_FOLLOW).setBodyBytes(builder3.build().toByteString());
                System.out.println("发送任务:"+id);
                NettyClient client=NettyConnectionFactory.getInstance().getConnection("127.0.0.1",2021);
                Object msg= NettyMsgService.sendSyncMsg(client,builder4.build());
                Dto.Frame frame= (Dto.Frame) msg;
                String ret=frame.getBody();
                System.out.println("服务器返回:"+ret);
                Thread.sleep(5000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void sendASyncMsg(){
        try {
            while (true){
                ScheduleDto.Schedule.Builder builder=ScheduleDto.Schedule.newBuilder();
                String id=String.valueOf(System.currentTimeMillis());
                builder.setId(id).setClassPath("com.github.liuche51.easyTask.test.task.CusTask1").setExecuteTime(1586078809995l)
                        .setTaskType("PERIOD").setPeriod(30).setUnit("SECONDS").setSource("127.0.0.1").setTransactionId(Util.generateTransactionId())
                        .setParam("birthday#;1986-1-1&;threadid#;1&;name#;Jack&;age#;32&");
                Dto.Frame.Builder builder1=Dto.Frame.newBuilder();
                builder1.setIdentity(StringConstant.EMPTY);
                builder1.setInterfaceName(NettyInterfaceEnum.TRAN_TRYSAVETASK).setBodyBytes(builder.build().toByteString());
                System.out.println("发送任务:"+id);
                NettyClient client=NettyConnectionFactory.getInstance().getConnection("127.0.0.1",2021);
                ChannelFuture future= NettyMsgService.sendASyncMsg(client,builder1.build());
                //这种异步消息回调方法，没法获取服务器返回的结果信息，只能知道是否完成异步通信。有点坑
                future.addListener(new GenericFutureListener<Future<? super Void>>() {
                    @Override
                    public void operationComplete(Future<? super Void> future) throws Exception {
                        if(future.isSuccess()){
                            System.out.println("服务器返回完成");
                        }

                    }
                });
                Thread.sleep(5000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
