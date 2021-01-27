package com.hudsun.flink.javaspi;

/**
 * @Author wangkai
 * @Time 2021/1/27 20:08
 */
public class TestMyServiceSystemProperty {

    public static void main(String[] args) {
        System.out.println("hello,world!\n ");

        String path1 = System.getProperty("sun.boot.class.path");
        System.out.println("BootStrapClassLoader 加载的资源:" + path1);

        String path2 = System.getProperty("java.ext.dirs");
        System.out.println("ExtClassLoader 加载的资源：" + path2);

        String path3 = System.getProperty("java.class.path");
        System.out.println("AppClassLoader 加载的资源:" + path3);
    }
}
