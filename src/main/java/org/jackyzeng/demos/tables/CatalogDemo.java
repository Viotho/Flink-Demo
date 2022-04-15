package org.jackyzeng.demos.tables;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class CatalogDemo {
    public static void main(String[] args) {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, configuration);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        // 创建一个HiveCatalog
        // 四个参数分别为：catalogName、databaseName、hiveConfDir、hiveVersion
        Catalog catalog = new HiveCatalog("customCatalog", null, "<path_of_hive_conf>", "<hive_version>");
        // 注册catalog，取名为mycatalog
        tEnv.registerCatalog("mycatalog", catalog);

        // 创建一个Database，取名为mydb
        tEnv.sqlUpdate("CREATE DATABASE mydb WITH (...)");


        // 创建一个Table，取名为mytable
        tEnv.sqlUpdate("CREATE TABLE mytable (name STRING, age INT) WITH (...)");

        // 返回所有Table
        tEnv.listTables();
    }
}
