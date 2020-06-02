package com.github.liuche51.easyTask.cluster.leader;

import com.github.liuche51.easyTask.backup.client.NettyClient;
import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dto.proto.Dto;
import com.github.liuche51.easyTask.register.ZKService;
import com.github.liuche51.easyTask.util.StringConstant;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * leader类
 */
public class LeaderUtil {
    private static final Logger log = Logger.getLogger(LeaderUtil.class);
    /**
     * 通知follows当前Leader位置。异步调用即可
     *
     * @return
     */
    public static boolean notifyFollowsLeaderPosition( List<Node> follows,int tryCount) {
        EasyTaskConfig.getInstance().getClusterPool().submit(new Runnable() {
            @Override
            public void run() {
                if (follows != null) {
                    follows.forEach(x -> {
                        notifyFollowLeaderPosition(x,tryCount);
                    });
                }
            }
        });
        return true;
    }
    /**
     *
     * @param follow
     * @param tryCount
     * @return
     */
    public static boolean notifyFollowLeaderPosition(Node follow,int tryCount) {
        if (tryCount == 0) return false;
        final boolean[] ret = {false};
        try {
            Dto.Frame.Builder builder = Dto.Frame.newBuilder();
            builder.setIdentity(StringConstant.EMPTY);
            builder.setInterfaceName(StringConstant.SYNC_LEADER_POSITION).setBody(EasyTaskConfig.getInstance().getzKServerName());
            ChannelFuture future=follow.getClient().sendASyncMsgWithoutPromise(builder.build());
            tryCount--;
            future.addListener(new GenericFutureListener<Future<? super Void>>() {
                @Override
                public void operationComplete(Future<? super Void> future) throws Exception {
                    if(future.isSuccess()){
                        ret[0] =true;
                    }
                }
            });
            if(ret[0])
                return true;
        } catch (Exception e) {
            tryCount--;
            log.error("notifyFollowLeaderPosition.tryCount="+tryCount, e);
        }
        return notifyFollowLeaderPosition(follow, tryCount);
    }

}
