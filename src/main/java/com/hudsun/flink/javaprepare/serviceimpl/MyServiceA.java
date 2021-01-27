package com.hudsun.flink.javaprepare.serviceimpl;

import com.hudsun.flink.javaprepare.serviceprovider.MyService;

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
