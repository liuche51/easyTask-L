package com.github.liuche51.easyTask.core;

import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.dto.zk.ZKFollow;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Util {
    public static AtomicLong GREACE = new AtomicLong(0);

    public static String generateUniqueId() {
        StringBuilder str = new StringBuilder(UUID.randomUUID().toString().replace("-", ""));
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

    public static String getLocalIP() throws UnknownHostException {
        String ip = InetAddress.getLocalHost().getHostAddress();
        String[] temp = ip.split("/");
        if (temp.length == 1) return temp[0];
        else if (temp.length == 2) return temp[1];
        else return ip;
    }

    public static String getLinuxLocalIP() throws SocketException {
        Enumeration allNetInterfaces = NetworkInterface.getNetworkInterfaces();
        InetAddress ip = null;
        while (allNetInterfaces.hasMoreElements()) {
            NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
            System.out.println(netInterface.getName());
            Enumeration addresses = netInterface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                ip = (InetAddress) addresses.nextElement();
                if (ip != null && ip instanceof Inet4Address) {
                    return ip.getHostAddress();
                }
            }
        }
        return null;
    }
    public static List<ZKFollow> nodeToZKFollow(List<Node> list){
        if(list==null) return null;
        List<ZKFollow> ret=new ArrayList<>(list.size());
        list.forEach(x->{
            ZKFollow temp=new ZKFollow(x.getHost(),x.getPort());
            ret.add(temp);
        });
        return ret;
    }
}
