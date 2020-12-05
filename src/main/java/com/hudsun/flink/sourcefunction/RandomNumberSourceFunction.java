package com.hudsun.flink.sourcefunction;

import cn.hutool.core.util.RandomUtil;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import scala.Tuple2;

/**
 * 随机数字产生器
 * 产生一个随机的用户id和随机的数字
 *
 * @Author wangkai
 * @Time 2020/12/5 13:19
 */
public class RandomNumberSourceFunction implements SourceFunction<Integer> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (running) {
            Thread.sleep(1000);
            ctx.collect(RandomUtil.randomInt(10000));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
