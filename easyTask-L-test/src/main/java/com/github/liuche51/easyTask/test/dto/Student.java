package com.github.liuche51.easyTask.test.dto;

import java.io.Serializable;
import java.util.Date;

public class Student implements Serializable {
    private String name;
    private Date birthday;
    private int age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
