package com.github.liuche51.easyTask.core.test;

import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.register.NodeData;
import com.github.liuche51.easyTask.register.RegisterCenter;
import com.github.liuche51.easyTask.register.ZKUtil;
import org.junit.Test;

import java.util.List;

public class UnitTest {
    private RegisterCenter zKServiceImpl=new RegisterCenter();
    @Test
    public void zkinit(){
        try {
            EasyTaskConfig.getInstance().setZKServerName("server1");
            ZKUtil.initZK();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void register(){
        try {
            EasyTaskConfig.getInstance().setZKServerName("server1");
            NodeData data=new NodeData();
            data.setBackupCount(0);
            data.setTaskCount(111);
            zKServiceImpl.register(data);
            getList();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void getList(){
        try {
            List<String> list= zKServiceImpl.getList();
            list.forEach(x->{
                System.out.println(x);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
