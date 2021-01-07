package com.hudsun.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author wangkai
 * @Time 2020/12/22 23:11
 */
public class DebeziumCDC {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings streamSettings = EnvironmentSettings.newInstance()
                .inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment =
                StreamTableEnvironment.create(env, streamSettings);
        tableEnvironment.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        // source properties
        String topicName = "dbserver1.inventory.customers";
        String bootStrpServers = "192.168.56.10:9092";
        String groupID = "testGroup";

        // sink properties
        String url = "jdbc:mysql://192.168.56.10:3306/inventory";
        String userName = "root";
        String password = "debezium";
        String mysqlSinkTable = "customers_copy";


        // create kafka source to read debezium data
        tableEnvironment.executeSql("CREATE TABLE customers (\n" +
                " id int,\n" +
                " first_name STRING,\n" +
                " last_name STRING,\n" +
                " email STRING \n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = '" + topicName + "',\n" +
                " 'properties.bootstrap.servers' = '" + bootStrpServers + "',\n"
                +
                " 'debezium-json.schema-include' = 'true',\n" +
                " 'properties.group.id' = '" + groupID + "',\n" +
                " 'format' = 'debezium-json'\n" +
                ")");

        // create jdbc sink to write data
        tableEnvironment.executeSql("CREATE TABLE customers_copy (\n" +
                " id int,\n" +
                " first_name STRING,\n" +
                " last_name STRING,\n" +
                " email STRING, \n" +
                " PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'jdbc',\n" +
                " 'url' = '" + url + "',\n" +
                " 'username' = '" + userName + "',\n" +
                " 'password' = '" + password + "',\n" +
                " 'table-name' = '" + mysqlSinkTable + "'\n" +
                ")");
        String updateSQL = "insert into customers_copy select * from customers";
        TableResult result = tableEnvironment.executeSql(updateSQL);

        env.execute("sync-flink-cdc");
    }
}
