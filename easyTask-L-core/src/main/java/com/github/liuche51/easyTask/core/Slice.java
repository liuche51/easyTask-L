package com.github.liuche51.easyTask.core;

import com.github.liuche51.easyTask.dto.Task;

import java.util.concurrent.ConcurrentSkipListMap;

class Slice {
    private ConcurrentSkipListMap<String, Task> list=new ConcurrentSkipListMap<String, Task>();;

    public ConcurrentSkipListMap<String, Task> getList() {
        return list;
    }

    public void setList(ConcurrentSkipListMap<String, Task> list) {
        this.list = list;
    }
}
