package org.jackyzeng.demos.tables;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class TableApiStructure {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, configuration);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);
//        tEnv.getConfig().setIdleStateRetention(Duration.ofHours(1));

//        // 使用connect函数连接外部系统
//        tEnv.connect(
//        new Kafka()
//                .version("universal")     // 必填，Kafka版本，合法的参数有"0.8", "0.9", "0.10", "0.11"或"universal"
//                .topic("user_behavior")   // 必填，Topic名
//                .startFromLatest()        // 首次消费时数据读取的位置
//                .property("zookeeper.connect", "localhost:2181")  // Kafka连接参数
//                .property("bootstrap.servers", "localhost:9092"))
//        // 序列化方式 可以是JSON、Avro等
//        .withFormat(new Json())
//        // 数据的Schema
//        .withSchema(new Schema()
//                        .field("user_id", DataTypes.BIGINT())
//                        .field("item_id", DataTypes.BIGINT())
//                        .field("category_id", DataTypes.BIGINT())
//                        .field("behavior", DataTypes.STRING())
//                        .field("ts", DataTypes.TIMESTAMP(3)))
//        // 临时表的表名，后续可以在SQL语句中使用这个表名
//        .createTemporaryTable("user_behavior");
//        Table userBehaviorTable = tEnv.from("user_behavior");

//        tEnv.connect(new FileSystem().path("..."))
//                .withFormat(new Csv().fieldDelimiter('|'))
//                .withSchema(schema)
//                .createTemporaryTable("CsvSinkTable");
//
//        // 执行查询操作，得到一个名为result的Table
//        Table result = ...
//        // 将result发送到名为CsvSinkTable的TableSink
//        result.insertInto("CsvSinkTable");

        List<Tuple4<Integer, Long, String, Timestamp>> list = new ArrayList<>();
        list.add(Tuple4.of(1, 1L, "Jack#22", Timestamp.valueOf("2020-03-06 00:00:00")));
        list.add(Tuple4.of(2, 2L, "John#19", Timestamp.valueOf("2020-03-06 00:00:01")));
        list.add(Tuple4.of(3, 3L, "nosharp", Timestamp.valueOf("2020-03-06 00:00:03")));

        DataStream<Tuple4<Integer, Long, String, Timestamp>> stream = env
                .fromCollection(list)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Integer, Long, String, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.getTime())
                );

        Table inputTable = tEnv.fromDataStream(stream).as("id", "long_id", "name", "ts.rowtime");
//        inputTable.filter($("")).groupBy($("")).select($("").sum().as("sum"));
        tEnv.createTemporaryView("input_table", inputTable);
        Table resultTable = tEnv.sqlQuery("SELECT id, s FROM input_table, LATERAL TABLE(Func(str)) AS T(s)");

//        // Table Api Using Window with ROWS in SQL Demo
//        tEnv.sqlQuery("SELECT \n" +
//                "    field1,\n" +
//                "    AGG_FUNCTION(field2) OVER (\n" +
//                "     [PARTITION BY (value_expression1,..., value_expressionN)] \n" +
//                "     ORDER BY timeAttr\n" +
//                "     ROWS \n" +
//                "     BETWEEN (UNBOUNDED | rowCount) PRECEDING AND CURRENT ROW) AS fieldName\n" +
//                "FROM tab1\n" +
//                "\n" +
//                "-- 使用AS\n" +
//                "SELECT \n" +
//                "    field1,\n" +
//                "    AGG_FUNCTION(field2) OVER w AS fieldName\n" +
//                "FROM tab1\n" +
//                "WINDOW w AS (\n" +
//                "     [PARTITION BY (value_expression1,..., value_expressionN)] \n" +
//                "     ORDER BY timeAttr\n" +
//                "     ROWS \n" +
//                "     BETWEEN (UNBOUNDED | rowCount) PRECEDING AND CURRENT ROW\n" +
//                ")");

//        // Table Api Using Window with RANGE in SQL Demo
//        tEnv.sqlQuery("SELECT \n" +
//                "    field1,\n" +
//                "    AGG_FUNCTION(field2) OVER (\n" +
//                "     [PARTITION BY (value_expression1,..., value_expressionN)] \n" +
//                "     ORDER BY timeAttr\n" +
//                "     RANGE\n" +
//                "     BETWEEN (UNBOUNDED | timeInterval) PRECEDING AND CURRENT ROW) AS fieldName\n" +
//                "FROM tab1\n" +
//                "\n" +
//                "-- 使用AS\n" +
//                "SELECT \n" +
//                "    field1,\n" +
//                "    AGG_FUNCTION(field2) OVER w AS fieldName\n" +
//                "FROM tab1\n" +
//                "WINDOW w AS (\n" +
//                "     [PARTITION BY (value_expression1,..., value_expressionN)] \n" +
//                "     ORDER BY timeAttr\n" +
//                "     RANGE\n" +
//                "     BETWEEN (UNBOUNDED | timeInterval) PRECEDING AND CURRENT ROW\n" +
//                ") ");

        DataStream<Row> resultStream = tEnv.toDataStream(resultTable);
//        DataStream<Row> appendStream = tEnv.toAppendStream(resultTable, Row.class);
//        DataStream<Tuple2<Boolean, Row>> retractStream = tEnv.toRetractStream(inputTable, Row.class);
//        DataStream<Row> changelogStream = tEnv.toChangelogStream(resultTable);
        resultStream.print();
        env.execute("table api");
    }
}
