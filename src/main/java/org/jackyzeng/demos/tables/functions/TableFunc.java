package org.jackyzeng.demos.tables.functions;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

// Table Function能够接收零到多个标量输入，输出零到多行，每行数据一到多列。
// Table Function更像是一个表，一般出现在FROM之后。
// 在SQL语句中，使用LATERAL TABLE(<TableFunctionName>)来调用这个Table Function。
public class TableFunc extends TableFunction<String> {

    // 按#切分字符串，输出零到多行
    public void eval(String str) {
        if (str.contains("#")) {
            String[] arr = str.split("#");
            for (String i: arr) {
                collect(i);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, configuration);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        List<Tuple4<Integer, Long, String, Timestamp>> list = new ArrayList<>();
        list.add(Tuple4.of(1, 1L, "Jack#22", Timestamp.valueOf("2020-03-06 00:00:00")));
        list.add(Tuple4.of(2, 2L, "John#19", Timestamp.valueOf("2020-03-06 00:00:01")));
        list.add(Tuple4.of(3, 3L, "nosharp", Timestamp.valueOf("2020-03-06 00:00:03")));

        DataStream<Tuple4<Integer, Long, String, Timestamp>> stream = env
                .fromCollection(list)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, Long, String, Timestamp>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Integer, Long, String, Timestamp> element) {
                        return element.f3.getTime();
                    }
                });
        // 获取Table
        Table table = tEnv.fromDataStream(stream).as("id", "long", "str", "ts.rowtime");
        tEnv.createTemporaryView("input_table", table);

        // 注册函数到Catalog中，指定名字为Func
        tEnv.createTemporarySystemFunction("Func", new TableFunc());

        // input_table与LATERAL TABLE(Func(str))进行JOIN
        Table tableFunc = tEnv.sqlQuery("SELECT id, s FROM input_table, LATERAL TABLE(Func(str)) AS T(s)");
        DataStream<Row> result = tEnv.toAppendStream(tableFunc, Row.class);
        result.print();

        env.execute("Table Function Demo");
    }
}
