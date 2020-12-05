package com.hudsun.flink.state;

import com.hudsun.flink.sourcefunction.RandomTuple2SourceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

/**
 * KeyedState 例子
 * 展示 MapState 基础用法
 *
 * @Author wangkai
 * @Time 2020/12/5 15:12
 */
public class KeyedStateForMapState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new RandomTuple2SourceFunction())
                .keyBy((e) -> e._1)
                .map(new UserNumberSumMapFunction())
                .print();

        env.execute("UserMaxNumber");
    }

    /**
     * 输入是一个用户和数字，输出是这个用户历史的最大数字
     */
    public static class UserNumberSumMapFunction
            extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private MapState<String, Integer> userNumberSum;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            MapStateDescriptor<String, Integer> mapStateDescriptor =
                    new MapStateDescriptor<>("UserMaxNumbers", String.class, Integer.class);
            userNumberSum = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
            String user = value._1;
            Integer number = value._2;

            Integer current = userNumberSum.get(user);
            if (current == null) {
                current = 0;
            }
            current = current + number;
            userNumberSum.put(user, current);

            return new Tuple2<>(user, current);
        }
    }
}
