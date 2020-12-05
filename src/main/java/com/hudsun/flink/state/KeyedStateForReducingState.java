package com.hudsun.flink.state;

import com.hudsun.flink.sourcefunction.RandomNumberSourceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

/**
 * Keyed State 的 Reducing State
 * @Author wangkai
 * @Time 2020/12/5 15:27
 */
public class KeyedStateForReducingState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new RandomNumberSourceFunction())
                .map((event) -> new Tuple2<String, Integer>("max", event))
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                })
                .keyBy((e) -> e._1)
                .map(new SumNumberUsingReducingStateMapFunction())
                .print();

        env.execute("ReducingFunction");
    }

    public static class SumNumberUsingReducingStateMapFunction
            extends RichMapFunction<Tuple2<String, Integer>, Integer> {

        private ReducingState<Integer> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ReducingStateDescriptor<Integer> reducingStateDescriptor =
                    new ReducingStateDescriptor<>("Sum Number", new ReduceFunction<Integer>() {
                @Override
                public Integer reduce(Integer value1, Integer value2) throws Exception {
                    return value1 + value2;
                }
            }, Integer.class);
            reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
        }

        @Override
        public Integer map(Tuple2<String, Integer> value) throws Exception {
            reducingState.add(value._2);
            return reducingState.get();
        }
    }
}
