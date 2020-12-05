package com.hudsun.flink.window;

import com.hudsun.flink.model.SimpleLogDetail;
import com.hudsun.flink.sourcefunction.RandomLogSourceFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Author wangkai
 * @Time 2020/11/28 18:54
 */
public class WindowCountExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 数据源使用自定义数据源，每1s发送一条随机消息
        env.addSource(new RandomLogSourceFunction())
                // 指定水印生成策略是，最大事件时间减去 5s，指定事件时间字段为 timestamp
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <SimpleLogDetail>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event,timestamp)->event.timestamp))
                // 按 消息分组
                .keyBy((event)->event.msg)
                // 定义一个10s的时间窗口
                .timeWindow(Time.seconds(10))
                // 统计消息出现的次数
                .sum("cnt")
                // 打印输出
                .print();

        env.execute("log_window_cnt");
    }
}
