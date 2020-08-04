package com.github.liuche51.easyTask.dto;

/**
 * 与目标主机的时钟差距
 */
public class ClockDiffer {
    /**
     * 当前节点与目标节点时间差值。单位秒
     * 正数代表目标节点时钟比当前节点慢
     */
    private long differSecond=0;

    public long getDifferSecond() {
        return differSecond;
    }

    public void setDifferSecond(long differSecond) {
        this.differSecond = differSecond;
    }
}
