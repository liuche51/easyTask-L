package com.github.liuche51.easyTask.core;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class Util {
    public static AtomicLong GREACE=new AtomicLong(0);
    public static String generateUniqueId(){
        StringBuilder str=new StringBuilder(UUID.randomUUID().toString().replace("-",""));
        str.append("-");
        str.append(Thread.currentThread().getId());
        return str.toString();
    }
    public static String getDefaultDbDirect() throws IOException {
        // 第二种：获取项目路径    D:\git\daotie\daotie
        File directory = new File("");// 参数为空
        String courseFile = directory.getCanonicalPath();
        return courseFile;
    }
}
