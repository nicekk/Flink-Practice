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
public class FlinkSqlWindowByProcessingTime {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,settings);

        String sourceDDL = "CREATE TABLE user_actions (\n" +
                "user_name string,\n" +
                "data string,\n" +
                "user_action_time as PROCTIME()\n" +
                "   ) WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'user_log',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'properties.bootstrap.servers' = '192.168.56.10:9092',\n" +
                "'format' = 'json',\n" +
                "'json.fail-on-missing-field' = 'false'\n" +
                ")";

        String sinkDDL = "CREATE TABLE user_action_result(\n" +
                "window_start TIMESTAMP(3),\n" +
                "cnt bigint\n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ")";

        String querySql =
                "INSERT INTO user_action_result\n" +
                "select * from (\n" +
                "SELECT TUMBLE_START(user_action_time, INTERVAL '10' SECOND) window_start, COUNT(DISTINCT user_name) cnt\n" +
                "FROM user_actions\n" +
                "GROUP BY TUMBLE(user_action_time, INTERVAL '10' SECOND)\n" +
                ") T1";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql(querySql);

    }
}
