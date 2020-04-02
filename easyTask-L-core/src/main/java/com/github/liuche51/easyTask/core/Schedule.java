package com.github.liuche51.easyTask.core;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

public class Schedule{
    /**
     * 任务截止运行时间
     */
    private long endTimestamp;
    private TaskType taskType=TaskType.ONECE;
    private boolean immediateExecute=false;
    private long period;
    private TimeUnit unit;
    private Runnable run;
   private ScheduleExt scheduleExt=new ScheduleExt();
    private Map<String,String> param;



    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }
    public TaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }
    public long getPeriod() {
        return period;
    }
    public boolean isImmediateExecute() {
        return immediateExecute;
    }

    public void setImmediateExecute(boolean immediateExecute) {
        this.immediateExecute = immediateExecute;
    }
    public void setPeriod(long period) {
        this.period = period;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public void setUnit(TimeUnit unit) {
        this.unit = unit;
    }
    public Map<String,String> getParam() {
        return param;
    }

    public ScheduleExt getScheduleExt() {
        return scheduleExt;
    }

    public void setScheduleExt(ScheduleExt scheduleExt) {
        this.scheduleExt = scheduleExt;
    }

    public void setParam(Map<String,String> param) {
        this.param = param;
    }
    public void save() {
        ScheduleDao.save(this);
    }
    /**
     * 获取周期性任务下次执行时间。已当前时间为基准计算下次而不是上次截止执行时间
     * @param period
     * @param unit
     * @return
     * @throws Exception
     */
    public static long getTimeStampByTimeUnit(long period,TimeUnit unit) throws Exception {
        if(period==0)
            throw new Exception("period can not zero");
        switch (unit)
        {
            case DAYS:
                return ZonedDateTime.now().plusDays(period).toInstant().toEpochMilli();
            case HOURS:
                return ZonedDateTime.now().plusHours(period).toInstant().toEpochMilli();
            case MINUTES:
                return ZonedDateTime.now().plusMinutes(period).toInstant().toEpochMilli();
            case SECONDS:
                return ZonedDateTime.now().plusSeconds(period).toInstant().toEpochMilli();
                default:throw new Exception("unSupport TimeUnit type");
        }
    }

    /**
     * 序列化Map
     * @param param
     * @return
     */
    public static String serializeMap(Map<String,String> param){
        if(param!=null&&param.size()>0){
            StringBuilder builder=new StringBuilder();
            for(Map.Entry<String,String> entry:param.entrySet()){
                builder.append(entry.getKey()).append("#;").append(entry.getValue()).append("&;");
            }
            return builder.toString();
        }else return "";
    }

    /**
     * 反序列化Map
     * @param param
     * @return
     */
    public static Map<String,String> deserializeMap(String param){
        if(param!=null&&!"".equals(param)){
            Map<String,String> map=new HashMap<>();
            String[] temp=param.split("&;");
            for(int i=0;i<temp.length;i++){
                String[] temp2=temp[i].split("#;");
                map.put(temp2[0],temp2[1]);
            }
            return map;
        }else return null;
    }
}
