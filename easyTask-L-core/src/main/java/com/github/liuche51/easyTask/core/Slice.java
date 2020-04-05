package com.github.liuche51.easyTask.core;

import com.github.liuche51.easyTask.dto.Schedule;

import java.util.concurrent.ConcurrentSkipListMap;

class Slice {
    private ConcurrentSkipListMap<String, Schedule> list=new ConcurrentSkipListMap<String,Schedule>();;

    public ConcurrentSkipListMap<String,Schedule> getList() {
        return list;
    }

    public void setList(ConcurrentSkipListMap<String,Schedule> list) {
        this.list = list;
    }
}
