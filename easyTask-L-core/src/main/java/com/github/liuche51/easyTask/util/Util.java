package com.github.liuche51.easyTask.util;

import com.github.liuche51.easyTask.cluster.ClusterService;
import com.github.liuche51.easyTask.cluster.Node;
import com.github.liuche51.easyTask.core.AnnularQueue;
import com.github.liuche51.easyTask.core.EasyTaskConfig;
import com.github.liuche51.easyTask.dto.zk.ZKHost;
import com.github.liuche51.easyTask.util.StringConstant;
import org.sqlite.SQLiteException;

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
    public static String generateTransactionId() {
        return "T"+generateUniqueId();
    }
    public static String generateIdentityId() {
        return "I"+generateUniqueId();
    }
    public static String getDefaultDbDirect() throws IOException {
        // 第二种：获取项目路径    D:\git\daotie\daotie
        File directory = new File("");// 参数为空
        String courseFile = directory.getCanonicalPath();
        return courseFile;
    }

    public static String getLocalIP() throws Exception {
        if(isLinux()){
            return getLinuxLocalIP();
        }else if(isWindows()){
            return getWindowsLocalIP();
        }else
            throw new Exception("Unknown System Type!Only support Linux and Windows System.");
    }
    /**
     * 获取windows下IP地址
     * 如果在Linux下，则结果都是127.0.0.1
     * @return
     * @throws UnknownHostException
     */
    private static String getWindowsLocalIP() throws UnknownHostException {
        String ip = InetAddress.getLocalHost().getHostAddress();
        String[] temp = ip.split("/");
        if (temp.length == 1) return temp[0];
        else if (temp.length == 2) return temp[1];
        else return ip;
    }

    /**
     * 获取Linux下的IP地址
     * 如果在windows下，则结果都是127.0.0.1
     * @return
     * @throws SocketException
     */
    private static String getLinuxLocalIP() throws SocketException {
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
    private static boolean isLinux() {
        return System.getProperty("os.name").toLowerCase().contains("linux");
    }
    private static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("windows");
    }
    /**
     * 集合对象转换
     *
     * @param list
     * @return
     */
    public static List<ZKHost> nodeToZKHost(List<Node> list) {
        if (list == null) return null;
        List<ZKHost> ret = new LinkedList<>();
        Iterator<Node> items = list.iterator();//防止remove操作导致线程不安全异常
        while (items.hasNext()) {
            Node x = items.next();
            ZKHost temp = new ZKHost(x.getHost(), x.getPort());
            ret.add(temp);
        }
        return ret;
    }

    /**
     * 集合对象转换
     *
     * @param list
     * @return
     */
    public static List<ZKHost> nodeToZKHost(Map<String, Node> list) {
        if (list == null) return null;
        List<ZKHost> ret = new ArrayList<>(list.size());
        for (Map.Entry<String, Node> key : list.entrySet()) {
            Node x = key.getValue();
            ZKHost temp = new ZKHost(x.getHost(), x.getPort());
            temp.setDataStatus(x.getDataStatus());
            ret.add(temp);
        }
        return ret;
    }

    /**
     * 集合对象转换
     *
     * @param list
     * @return
     */
    public static List<Node> zKHostToNode(List<ZKHost> list) {
        if (list == null) return null;
        List<Node> ret = new ArrayList<>(list.size());
        list.forEach(x -> {
            Node temp = new Node(x.getHost(), x.getPort());
            ret.add(temp);
        });
        return ret;
    }

    /**
     * 集合对象转换
     *
     * @param list
     * @return
     */
    public static Map<String, Node> zKHostToNodes(List<ZKHost> list) {
        if (list == null) return null;
        Map<String, Node> ret = new HashMap<>();
        list.forEach(x -> {
            Node node = new Node(x.getHost(), x.getPort());
            ret.put(x.getHost() + x.getPort(), node);
        });
        return ret;
    }

    /**
     * 获取任务来源的拼接字符串
     * @param oldSource
     * @return
     */
    public static String getSource(String oldSource) throws Exception {
        String source=StringConstant.EMPTY;
        if(oldSource==null||oldSource== StringConstant.EMPTY)
            source= AnnularQueue.getInstance().getConfig().getAddress();
        else
            source=AnnularQueue.getInstance().getConfig().getAddress()+"<-"+oldSource;
        return source;
    }
}
