package com.hudsun.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 消费 kafka 到 控制台
 *
 * @Author wangkai
 * @Time 2021/1/4 17:32
 */
public class FlinkSqlKafka2Kafka {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,settings);

        String sourceDDL = "CREATE TABLE user_log (\n" +
                "    user_id bigint,\n" +
                "    item_id bigint,\n" +
                "    ts TIMESTAMP\n" +
                "   ) WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'user_log',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'properties.bootstrap.servers' = '192.168.56.10:9092',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false'\n" +
                ")";

        String sinkDDL = "CREATE TABLE user_log_result (\n" +
                "                    user_id bigint,\n" +
                "                    cnt bigint\n" +
                "                   ) WITH (\n" +
                "                    'connector' = 'kafka',\n" +
                "                    'topic' = 'user_log_result',\n" +
                "                    'scan.startup.mode' = 'latest-offset',\n" +
                "                    'properties.bootstrap.servers' = '192.168.56.10:9092',\n" +
                "                    'format' = 'json'" +
                "                )";

//        String querySql = "insert into user_log_result select user_id,item_id from user_log ";
        String querySql = "insert into user_log_result select user_id,count(1) cnt from user_log group by user_id";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql(querySql);

    }
}
