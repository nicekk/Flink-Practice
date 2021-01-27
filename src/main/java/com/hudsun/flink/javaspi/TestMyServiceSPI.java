package com.hudsun.flink.javaspi;

import com.hudsun.flink.javaspi.serviceprovider.MyService;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @Author wangkai
 * @Time 2021/1/27 15:12
 */
public class TestMyServiceSPI {

    public static void main(String[] args) {
        ServiceLoader<MyService> services = ServiceLoader.load(MyService.class);
        Iterator<MyService> iterator = services.iterator();
        while (iterator.hasNext()) {
            MyService myService = iterator.next();
            myService.doSomething();
        }
    }
}
