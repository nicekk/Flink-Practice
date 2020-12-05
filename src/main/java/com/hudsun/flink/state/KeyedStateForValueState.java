package com.hudsun.flink.state;

import com.hudsun.flink.sourcefunction.RandomNumberSourceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

/**
 * KeyedState中的 ValueState 实战源码
 * 实现的功能，读取最大值，然后输出
 *
 * @Author wangkai
 * @Time 2020/12/5 13:08
 */
public class KeyedStateForValueState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new RandomNumberSourceFunction())
                .map((event) -> new Tuple2<String, Integer>("max", event))
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                })
                .keyBy((e) -> e._1)
                .map(new SumRichMapFunction())
                .print();

        env.execute("MaxCount");
    }

    /**
     * 统计最大数字的富Map类
     */
    public static class SumRichMapFunction extends RichMapFunction<Tuple2<String, Integer>, Integer> {

        private ValueState<Integer> sum;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<Integer>("max of numbers", Integer.class);
            sum = getRuntimeContext().getState(descriptor);
        }

        @Override
        public Integer map(Tuple2<String, Integer> tuple2) throws Exception {
            Integer current = sum.value();
            Integer input = tuple2._2;
            if (current == null) {
                current = 0;
            }
            current = current + input;
            sum.update(current);
            return current;
        }
    }

}
