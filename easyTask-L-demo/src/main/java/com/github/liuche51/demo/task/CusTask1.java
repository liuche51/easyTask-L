package com.github.liuche51.demo.task;


import com.github.liuche51.easyTask.dto.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CusTask1 extends Task implements Runnable {
    private static Logger log = LoggerFactory.getLogger(CusTask1.class);

    @Override
    public void run() {
        Map<String, String> param = getParam();
        if (param != null && param.size() > 0)
            log.info("任务1已执行!姓名:{} 生日:{} 年龄:{} 线程ID:{}", param.get("name"), param.get("birthday"), param.get("age"),param.get("threadid"));

    }
}
