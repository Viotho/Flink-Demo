package org.jackyzeng.demos.tables.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.*;

public class TopTwo {

    /**
     * Accumulator for Top2.
     */
    public static class Top2Accum {
        public Integer first;
        public Integer second;
    }

    /**
     * The top2 user-defined table aggregate function.
     */
    public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accum> {

        @Override
        public Top2Accum createAccumulator() {
            Top2Accum acc = new Top2Accum();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }


        public void accumulate(Top2Accum acc, Integer v) {
            if (v > acc.first) {
                acc.second = acc.first;
                acc.first = v;
            } else if (v > acc.second) {
                acc.second = v;
            }
        }

        public void merge(Top2Accum acc, java.lang.Iterable<Top2Accum> iterable) {
            for (Top2Accum otherAcc : iterable) {
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            }
        }

        public void emitValue(Top2Accum acc, Collector<Tuple2<Integer, Integer>> out) {
            // emit the value and rank
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }

    public static void main(String[] args) {
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, configuration);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);
        // 注册函数
        tEnv.createTemporarySystemFunction("top2", new Top2());

        DataStream<Integer> dataStream = env.fromElements(3, 5, 4, 2, 7, 8);
        // 初始化表
        Table table = tEnv.fromDataStream(dataStream);

        // 使用函数
        table.groupBy($("key"))
                .flatAggregate($("top2(a) as (v, rank)"))
                .select($("key"), $("v"), $("rank"));
    }
}
