package com.hudsun.flink.sourcefunction;

import cn.hutool.core.util.RandomUtil;
import com.hudsun.flink.model.SimpleLogDetail;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 每 1 s 产生一个随机消息
 *
 * @Author wangkai
 * @Time 2020/12/5 13:09
 */
public class RandomLogSourceFunction implements SourceFunction<SimpleLogDetail> {
    private boolean running = true;

    @Override
    public void run(SourceContext<SimpleLogDetail> ctx) throws Exception {
        while (true) {
            Thread.sleep(1000);
            ctx.collect(new SimpleLogDetail(RandomUtil.randomString(1), 1, System.currentTimeMillis()));
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
