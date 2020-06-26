package com.github.liuche51.easyTask.util.exception;

/**
 * 连接异常
 * 比如节点宕机了。这种异常算正常异常，系统会进入选举流程
 */
public class ConnectionException extends Exception{
    public ConnectionException(String message){
        super(message);
    }
}
