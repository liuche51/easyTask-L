package com.github.liuche51.easyTask.core;


import com.github.liuche51.easyTask.netty.server.NettyServer;
import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.dao.DbInit;
import com.github.liuche51.easyTask.dto.Task;
import com.github.liuche51.easyTask.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.*;

/**
 * 环形任务队列
 */
public class AnnularQueue {
    private static Logger log = LoggerFactory.getLogger(AnnularQueue.class);
    private static AnnularQueue singleton = null;
    private EasyTaskConfig config=null;
    public volatile boolean isRunning = false;//防止多线程运行环形队列

    //测试时可能用到
   /* public void setConfig(EasyTaskConfig config) {
        this.config = config;
    }*/

    public EasyTaskConfig getConfig() {
        return config;
    }

    private Slice[] slices = new Slice[60];
    public Slice[] getSlices() {
        return slices;
    }
    public static AnnularQueue getInstance() {
        if (singleton == null) {
            synchronized (AnnularQueue.class) {
                if (singleton == null) {
                    singleton = new AnnularQueue();
                }
            }
        }
        return singleton;
    }

    private AnnularQueue() {
        for (int i = 0; i < slices.length; i++) {
            slices[i] = new Slice();
        }
    }
    public void start(EasyTaskConfig config) throws Exception {
        if(config==null)
            throw new Exception("config is null,please set a EasyTaskConfig!");
        EasyTaskConfig.validateNecessary(config);
        this.config=config;
        Thread th1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    runQueue();
                } catch (Exception e) {
                    log.error("AnnularQueue start fail.", e);
                }finally {
                    isRunning = false;
                }
            }
        });
        th1.start();
    }

    /**
     * start the AnnularQueue
     */
    private synchronized void runQueue() throws Exception {
        //避免重复执行
        if (isRunning)
            return;
        DbInit.init();
        NettyServer.getInstance().run();//启动组件的Netty服务端口
        ClusterService.initCurrentNode();
        isRunning = true;
        int lastSecond = 0;
        while (true) {
            int second = ZonedDateTime.now().getSecond();
            if (second == lastSecond) {
                Thread.sleep(500l);
                continue;
            }
            Slice slice = slices[second];
            log.debug("已执行时间分片:{}，任务数量:{}", second, slice.getList() == null ? 0 : slice.getList().size());
            lastSecond = second;
            config.getDispatchs().submit(new Runnable() {
                public void run() {
                    ConcurrentSkipListMap<String, Task> list = slice.getList();
                    List<Task> periodSchedules = new LinkedList<>();
                    for (Map.Entry<String, Task> entry : list.entrySet()) {
                        Task s = entry.getValue();
                        //因为计算时有一秒钟内的精度问题，所以判断时当前时间需多补上一秒。这样才不会导致某些任务无法得到及时的执行
                        if (System.currentTimeMillis() + 1000l >= s.getEndTimestamp()) {
                            Runnable proxy = (Runnable) new ProxyFactory(s).getProxyInstance();
                            config.getWorkers().submit(proxy);
                            if (TaskType.PERIOD.equals(s.getTaskType()))//周期任务需要重新提交新任务
                                periodSchedules.add(s);
                            list.remove(entry.getKey());
                            log.debug("工作任务:{} 已提交分片:{}", s.getTaskExt().getId(), second);
                        }
                        //因为列表是已经按截止执行时间排好序的，可以节省后面元素的过期判断
                        else break;
                    }
                    submitNewPeriodSchedule(periodSchedules);
                }
            });
        }
    }

    /**
     * 客户端提交任务。允许线程等待，直到easyTask组件启动完成
     *
     * @param task
     * @return
     * @throws Exception
     */
    public String submitAllowWait(Task task) throws Exception {
        while (!isRunning) {
            Thread.sleep(1000l);//如果未启动则休眠1s
        }
        return this.submit(task);
    }

    /**
     * 客户端提交任务。如果easyTask组件未启动，则抛出异常
     *
     * @param task
     * @return
     * @throws Exception
     */
    public String submit(Task task) throws Exception {
        if (!isRunning) throw new Exception("the easyTask has not started,please wait a moment!");
        task.getTaskExt().setId(Util.generateUniqueId());
        String path = task.getClass().getName();
        task.getTaskExt().setTaskClassPath(path);
        //周期任务，且为非立即执行的（EndTimestamp>当前时间），尽可能早点计算其下一个执行时间。免得因为持久化导致执行时间延迟
        if (task.getTaskType().equals(TaskType.PERIOD)&&System.currentTimeMillis()<task.getEndTimestamp()) {
            task.setEndTimestamp(Task.getNextExcuteTimeStamp(task.getPeriod(), task.getUnit()));
        }
        //以下两行代码不要调换，否则可能发生任务已经执行完成，而任务尚未持久化，导致无法执行删除持久化的任务风险
        ClusterService.saveTask(task);
        submitAddSlice(task);
        return task.getTaskExt().getId();
    }

    /**
     * 删除已提交任务。
     * 包括从环形队列中删除和持久化删除任务
     * @param taskId
     * @throws Exception
     */
    public void delete(String taskId) throws Exception {
        if (!isRunning) throw new Exception("the easyTask has not started,please wait a moment!");
        boolean ret=ClusterService.deleteTask(taskId);
        if(!ret)
            throw new Exception("delete failed! please try agin.");
        boolean hasDel=false;
        for(Slice slice:slices){
            ConcurrentSkipListMap<String, Task> list=slice.getList();
            Iterator<Map.Entry<String, Task>> items=list.entrySet().iterator();
            while (items.hasNext()){
                Map.Entry<String, Task> item=items.next();
                if(item.getValue().getTaskExt().getId().endsWith(taskId))
                {
                    items.remove();
                    hasDel=true;
                    log.debug("the taskId="+taskId+" has removed from AnnularQueue!");
                    break;
                }
            }
            if(hasDel) break;
        }
    }
    /**
     * 新leader将旧leader的备份数据重新提交给自己
     * 任务ID保持不变。老周期任务不考虑立即执行的情况
     * @param task
     * @return
     * @throws Exception
     */
    public String submitForInner(Task task) throws Exception {
        task.getTaskExt().setId(task.getTaskExt().getId());
        if (task.getTaskType().equals(TaskType.PERIOD)) {
            task.setEndTimestamp(Task.getNextExcuteTimeStamp(task.getPeriod(), task.getUnit()));
        }
        //以下两行代码不要调换，否则可能发生任务已经执行完成，而任务尚未持久化，导致无法执行删除持久化的任务风险
        ClusterService.saveTask(task);
        submitAddSlice(task);
        return task.getTaskExt().getId();
    }

    /**
     * 批量创建新周期任务
     *
     * @param list
     */
    private void submitNewPeriodSchedule(List<Task> list) {
        for (Task schedule : list) {
            try {
                schedule.setEndTimestamp(Task.getNextExcuteTimeStamp(schedule.getPeriod(), schedule.getUnit()));
                int slice = AddSlice(schedule);
                log.debug("已重新提交周期任务:{}，所属分片:{}，线程ID:{}", schedule.getTaskExt().getId(), slice, Thread.currentThread().getId());
            } catch (Exception e) {
                log.error("submitNewPeriodSchedule exception！", e);
            }
        }
    }
    /**
     * 将任务添加到时间分片中去。
     *
     * @param task
     * @return
     */
    private int AddSlice(Task task) throws Exception {
        ZonedDateTime time = ZonedDateTime.ofInstant(new Timestamp(task.getEndTimestamp()).toInstant(), ZoneId.systemDefault());
        int second = time.getSecond();
        Slice slice = slices[second];
        ConcurrentSkipListMap<String, Task> list2 = slice.getList();
        list2.put(task.getEndTimestamp() + "-" + Util.GREACE.getAndIncrement(), task);
        log.debug("已添加类型:{}任务:{}，所属分片:{} 预计执行时间:{} 线程ID:{}", task.getTaskType().name(), task.getTaskExt().getId(), time.getSecond(), time.toLocalTime(), Thread.currentThread().getId());
        return second;
    }

    /**
     * 提交任务到时间轮分片
     * 提交到分片前需要做的一些逻辑判断
     * @param task
     * @throws Exception
     */
    private void submitAddSlice(Task task) throws Exception {
        //立即执行的任务，第一次不走时间分片，直接提交执行。一次性和周期性任务都通过EndTimestamp判断是否需要立即执行
        if (System.currentTimeMillis()+1000l>=task.getEndTimestamp()) {
            log.debug("立即执行类工作任务:{}已提交代理执行", task.getTaskExt().getId());
            Runnable proxy = (Runnable) new ProxyFactory(task).getProxyInstance();
            config.getWorkers().submit(proxy);
            //如果是一次性任务，则不用继续提交到时间分片中了
            if (task.getTaskType().equals(TaskType.ONECE)) {
                return;
            }
            //前面只处理了周期任务非立即执行的情况。这里处理立即执行的情况下。需要重新设置下一个执行周期
            else if(task.getTaskType().equals(TaskType.PERIOD)){
                task.setEndTimestamp(Task.getNextExcuteTimeStamp(task.getPeriod(), task.getUnit()));
            }
        }
        AddSlice(task);
    }

    /**
     * 清空所有任务
     */
    public void clearTask() {
        for (Slice s : slices) {
            s.getList().clear();
        }
    }
}
