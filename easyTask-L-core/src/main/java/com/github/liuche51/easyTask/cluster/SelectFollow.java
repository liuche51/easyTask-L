package com.github.liuche51.easyTask.cluster;

import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.register.RegisterCenter;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class SelectFollow {

    public static List<String> findFollow() {
        List<String> list = RegisterCenter.getList();
        List<String> follows = new LinkedList<String>();
        int count= EasyTaskConfig.getInstance().getBackupCount();
        if (list.size() == 0) return follows;
        else if (list.size() == 1&&count>0) {
            follows.add(list.get(0));
            return follows;
        } else {
            Random random = new Random();
            for (int i = 0; i <count; i++) {
                int index = random.nextInt(list.size() + 1);
                follows.add(list.get(index));
                list.remove(index);
            }
        }
        return follows;
    }
}
