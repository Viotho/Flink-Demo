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
import org.apache.flink.table.functions.AggregateFunction;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

// 聚合函数一般将多行数据进行聚合，输出一个标量。
public class WeightedAvg extends AggregateFunction<Double, WeightedAvg.WeightedAvgAccum> {

    @Override
    public WeightedAvgAccum createAccumulator() {
        return new WeightedAvgAccum();
    }

    // 需要物化输出时，getValue方法会被调用
    @Override
    public Double getValue(WeightedAvgAccum acc) {
        if (acc.weight == 0) {
            return null;
        } else {
            return (double) acc.sum / acc.weight;
        }
    }

    // 新数据到达时，更新ACC
    public void accumulate(WeightedAvgAccum acc, long iValue, long iWeight) {
        acc.sum += iValue * iWeight;
        acc.weight += iWeight;
    }

    // 用于BOUNDED OVER WINDOW，将较早的数据剔除
    public void retract(WeightedAvgAccum acc, long iValue, long iWeight) {
        acc.sum -= iValue * iWeight;
        acc.weight -= iWeight;
    }

    // 将多个ACC合并为一个ACC
    public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
        for (WeightedAvgAccum a : it) {
            acc.weight += a.weight;
            acc.sum += a.sum;
        }
    }

    // 重置ACC
    public void resetAccumulator(WeightedAvgAccum acc) {
        acc.weight = 0L;
        acc.sum = 0L;
    }

    /**
     * 累加器 Accumulator
     * sum: 和
     * weight: 权重
     */
    public static class WeightedAvgAccum {
        public long sum = 0;
        public long weight = 0;
    }

    public static void main(String[] args) {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, configuration);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);

        List<Tuple4<Integer, Long, Long, Timestamp>> list = new ArrayList<>();
        list.add(Tuple4.of(1, 100L, 1L, Timestamp.valueOf("2020-03-06 00:00:00")));
        list.add(Tuple4.of(1, 200L, 2L, Timestamp.valueOf("2020-03-06 00:00:01")));
        list.add(Tuple4.of(3, 300L, 3L, Timestamp.valueOf("2020-03-06 00:00:13")));

        DataStream<Tuple4<Integer, Long, Long, Timestamp>> stream = env
                .fromCollection(list)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, Long, Long, Timestamp>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Integer, Long, Long, Timestamp> element) {
                        return element.f3.getTime();
                    }
                });

        Table table = tEnv.fromDataStream(stream).as("id", "v", "w", "ts.rowtime");
        tEnv.createTemporaryView("input_table", table);
        tEnv.createTemporarySystemFunction("WeightAvg", new WeightedAvg());
        Table agg = tEnv.sqlQuery("SELECT id, WeightAvg(v, w) FROM input_table GROUP BY id");
    }
}
