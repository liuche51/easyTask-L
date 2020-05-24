package com.github.liuche51.easyTask.cluster;

import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dao.BackupServerDao;
import com.github.liuche51.easyTask.dto.BackupServer;
import com.github.liuche51.easyTask.register.ZKService;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class SelectFollow {

    public static void init(){

    }
    public static List<String> findFollow() {
        List<String> list = ZKService.getChildrenByNameSpase();
        //排除自己
        Optional<String> temp=list.stream().filter(x->x.equals(EasyTaskConfig.getInstance().getzKServerName())).findFirst();
        if(temp.isPresent())
            list.remove(temp.get());
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
    public static  List<BackupServer> saveBackupServers(List<String> list){
        List<BackupServer> servers=new LinkedList<>();
        for (String server:list){
            BackupServer server1=new BackupServer(server);
            BackupServerDao.save(server1);
            servers.add(server1);
        }
        return servers;
    }

}
