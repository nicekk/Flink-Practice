package com.hudsun.flink.javaspi.serviceimpl;

import com.hudsun.flink.javaspi.serviceprovider.MyService;

/**
 * 实现类 A
 *
 * @Author wangkai
 * @Time 2021/1/27 15:11
 */
public class MyServiceA implements MyService {

    @Override
    public void doSomething() {
        System.out.println("Hello,MyService A!");
    }
}
