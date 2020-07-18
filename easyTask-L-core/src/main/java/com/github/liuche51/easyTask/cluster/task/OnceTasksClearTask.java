package com.github.liuche51.easyTask.cluster.task;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;

import java.util.Iterator;

/**
 * 后台一次性的任务清理掉已经运行完退出的线程对象
 * 后台一次性任务线程对象在启动时都会加入到一个线程集合中。以便在集群初始化时，设置线程退出的状态
 */
public class OnceTasksClearTask extends TimerTask {
    @Override
    public void run() {
        while (!isExit()) {
            try {
                Iterator<OnceTask> items=ClusterService.onceTasks.iterator();
                while (items.hasNext()){
                    OnceTask one=items.next();
                    if(one.isExit())
                        items.remove();
                }
            } catch (Exception e) {
                log.error("", e);
            }
            try {
                Thread.sleep(AnnularQueue.getInstance().getConfig().getClearScheduleBakTime());
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }
}
