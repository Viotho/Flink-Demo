package org.jackyzeng.demos.jobs;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.concurrent.TimeUnit;

public class CheckpointAndRestartSettings {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStateBackend(new HashMapStateBackend());
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://node:25000/flink/checkpoint");
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointTimeout(3600 * 1000);
        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000);
        checkpointConfig.setMaxConcurrentCheckpoints(3);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setTolerableCheckpointFailureNumber(10);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,
                Time.of(10L, TimeUnit.SECONDS)));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)));
        env.setRestartStrategy(RestartStrategies.noRestart());
    }
}
