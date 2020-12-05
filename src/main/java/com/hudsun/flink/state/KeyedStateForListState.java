package com.hudsun.flink.state;

import com.hudsun.flink.sourcefunction.RandomNumberSourceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * KeyedState 类型是 ListState
 *
 * @Author wangkai
 * @Time 2020/12/5 14:19
 */
public class KeyedStateForListState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new RandomNumberSourceFunction())
                .map((event) -> new Tuple2<String, Integer>("max", event))
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                })
                .keyBy((e) -> e._1)
                .map(new GreaterThanRichMapFunction())
                .print();

        env.execute("GreaterThan");
    }

    public static class GreaterThanRichMapFunction extends RichMapFunction<Tuple2<String, Integer>, List<Integer>> {

        private ListState<Integer> greaterThan;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<Integer> listStateDescriptor = new ListStateDescriptor<>("myList", Integer.class);
            greaterThan = getRuntimeContext().getListState(listStateDescriptor);
        }

        @Override
        public List<Integer> map(Tuple2<String, Integer> value) throws Exception {
            Integer current = value._2;
            if (current > 1000) {
                greaterThan.add(current);
            }
            List<Integer> results = new ArrayList<>();
            greaterThan.get().forEach((e) -> results.add(e));
            return results;
        }
    }

}
