package org.jackyzeng.demos.tables.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str) {
        for (String s : str.split(" ")) {
            // use collect(...) to emit a row
            collect(Row.of(s, s.length()));
        }
    }

    public static void main(String[] args) {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, configuration);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        // 在 Table API 里不经注册直接“内联”调用函数
        tEnv.from("MyTable").joinLateral(call(SplitFunction.class, $("myField")))
                .select($("myField"), $("word"), $("length"));

        tEnv.from("MyTable").leftOuterJoinLateral(call(SplitFunction.class, $("myField")))
                .select($("myField"), $("word"), $("length"));

        // 在 Table API 里重命名函数字段
        tEnv.from("MyTable").leftOuterJoinLateral(call(SplitFunction.class, $("myField")).as("newWord", "newLength"))
                .select($("myField"), $("newWord"), $("newLength"));

        // 注册函数
        tEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        // 在 Table API 里调用注册好的函数
        tEnv.from("MyTable").joinLateral(call("SplitFunction", $("myField")))
                .select($("myField"), $("word"), $("length"));
        tEnv.from("MyTable").leftOuterJoinLateral(call("SplitFunction", $("myField")))
                .select($("myField"), $("word"), $("length"));

        // 在 SQL 里调用注册好的函数
        tEnv.sqlQuery(
                "SELECT myField, word, length " +
                        "FROM MyTable, LATERAL TABLE(SplitFunction(myField))");
        tEnv.sqlQuery(
                "SELECT myField, word, length " +
                        "FROM MyTable " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) ON TRUE");

        // 在 SQL 里重命名函数字段
        tEnv.sqlQuery(
                "SELECT myField, newWord, newLength " +
                        "FROM MyTable " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) AS T(newWord, newLength) ON TRUE");
    }
}


