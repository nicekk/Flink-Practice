package com.hudsun.flink.cdc;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author wangkai
 * @Time 2020/12/22 23:32
 */
public class SimpleConsumer {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties=new Properties();
        properties.setProperty("bootstrap.servers","192.168.56.10:9092");
        properties.setProperty("group.id","flink-test");
        FlinkKafkaConsumer<String> consumer= new FlinkKafkaConsumer<>("dbserver1.inventory.customers", new SimpleStringSchema(), properties);
//		consumer.setStartFromEarliest();
        DataStream<String> stream=env.addSource(consumer);
        stream.print();
        // execute program
        env.execute("debezium-test");
    }
}
