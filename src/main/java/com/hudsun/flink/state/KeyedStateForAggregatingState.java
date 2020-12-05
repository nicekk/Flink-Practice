package com.hudsun.flink.state;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.StrUtil;
import com.hudsun.flink.model.UserNumber;
import com.hudsun.flink.model.UserNumberCountAccumulator;
import com.hudsun.flink.sourcefunction.RandomTuple2SourceFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author wangkai
 * @Time 2020/12/5 15:33
 */
public class KeyedStateForAggregatingState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new RandomTuple2SourceFunction())
                .map((event) -> new UserNumber(event._1, Convert.toLong(event._2)))
                .keyBy(UserNumber::getUser)
                .map(new SumNumberUsingAggregateMapFunction())
                .print();

        env.execute("SumNumberUsingAggregateMapFunction");
    }

    public static class SumNumberUsingAggregateMapFunction extends RichMapFunction<UserNumber, UserNumberCountAccumulator> {

        private AggregatingState<UserNumber, UserNumberCountAccumulator> aggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            AggregatingStateDescriptor<UserNumber, UserNumberCountAccumulator, UserNumberCountAccumulator> aggregatingStateDescriptor =
                    new AggregatingStateDescriptor<>(
                            "mySumAndCount",
                            new AggregateFunction<UserNumber, UserNumberCountAccumulator, UserNumberCountAccumulator>() {
                                @Override
                                public UserNumberCountAccumulator createAccumulator() {
                                    return new UserNumberCountAccumulator();
                                }

                                @Override
                                public UserNumberCountAccumulator add(UserNumber value, UserNumberCountAccumulator accumulator) {
                                    if (StrUtil.isEmpty(accumulator.getUser())) {
                                        accumulator.setUser(value.getUser());
                                    }
                                    accumulator.setCnt(accumulator.getCnt() == null ? 0 : accumulator.getCnt() + 1);
                                    accumulator.setNumber(accumulator.getNumber() == null ? 0 : accumulator.getNumber() + value.getNumber());
                                    return accumulator;
                                }

                                @Override
                                public UserNumberCountAccumulator getResult(UserNumberCountAccumulator accumulator) {
                                    return accumulator;
                                }

                                @Override
                                public UserNumberCountAccumulator merge(UserNumberCountAccumulator a, UserNumberCountAccumulator b) {
                                    a.setCnt(a.getCnt() + b.getCnt());
                                    a.setNumber(a.getNumber() + b.getNumber());
                                    return a;
                                }
                            },
                            UserNumberCountAccumulator.class);
            aggregatingState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);
        }

        @Override
        public UserNumberCountAccumulator map(UserNumber userNumber) throws Exception {
            aggregatingState.add(userNumber);
            return aggregatingState.get();
        }
    }
}
