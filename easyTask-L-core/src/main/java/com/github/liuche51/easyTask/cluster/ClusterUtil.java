package com.github.liuche51.easyTask.cluster;

import com.github.liuche51.easyTask.netty.client.NettyClient;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.dto.proto.ResultDto;
import com.github.liuche51.easyTask.netty.client.NettyMsgService;
import com.github.liuche51.easyTask.util.StringConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterUtil {
    private static final Logger log = LoggerFactory.getLogger(ClusterUtil.class);

    /**
     * 带重试次数的同步消息发送
     *
     * @param client
     * @param msg
     * @param tryCount
     * @return
     */
    public static boolean sendSyncMsgWithCount(NettyClient client, Object msg, int tryCount) {
        if (tryCount == 0) return false;
        String error = StringConstant.EMPTY;
        Object ret=null;
        Dto.Frame frame=null;
        try {
            ret =  NettyMsgService.sendSyncMsg(client,msg);
            frame = (Dto.Frame) ret;
            ResultDto.Result result = ResultDto.Result.parseFrom(frame.getBodyBytes());
            if (StringConstant.TRUE.equals(result.getResult()))
                return true;
            else
                error = result.getMsg();
        }
        catch (Exception e) {
            log.error("sendSyncMsgWithCount exception!error=" + error, e);
        }
        finally {
            tryCount--;
        }
        log.info("sendSyncMsgWithCount error" + error + ",tryCount=" + tryCount + ",objectHost=" + client.getObjectAddress());
        return sendSyncMsgWithCount(client, msg, tryCount);

    }
}
