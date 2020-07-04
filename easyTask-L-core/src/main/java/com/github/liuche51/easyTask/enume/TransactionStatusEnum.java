package com.github.liuche51.easyTask.enume;

/**
 * TCC事务状态
 */
public class TransactionStatusEnum {
    /**
     * 第一阶段
     */
    public static final short TRIED=1;
    /**
     * 第二阶段
     */
    public static final short CONFIRM=2;
    /**
     * 取消阶段
     */
    public static final short CANCEL=3;
    /**
     * 已完成
     */
    public static final short FINISHED=4;
}
