package com.github.liuche51.easyTask.util;

import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateUtils {
    public static String getCurrentDateTime(){
      return   ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
    public static ZonedDateTime parse(String dateTimeStr){
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
       return ZonedDateTime.parse(dateTimeStr, df.withZone(ZoneId.systemDefault()));
    }
    public static long getTimeStamp(ZonedDateTime dateTime){
        return ZonedDateTime.now().toInstant().toEpochMilli();
    }

    /**
     * zk心跳时间是否超时了死亡时间阈值
     * @param dateTime
     * @return
     */
    public static boolean isGreaterThanDeadTime(String dateTime){
        if(ZonedDateTime.now().minusSeconds(AnnularQueue.getInstance().getConfig().getDeadTimeOut())
                .compareTo(DateUtils.parse(dateTime)) > 0)
            return true;
        else return false;
    }

    /**
     * zk心跳时间是否超时了失效时间阈值
     * @param dateTime
     * @return
     */
    public static boolean isGreaterThanLoseTime(String dateTime){
        if(ZonedDateTime.now().minusSeconds(AnnularQueue.getInstance().getConfig().getLoseTimeOut())
                .compareTo(DateUtils.parse(dateTime)) > 0)
            return true;
        else return false;
    }
}
