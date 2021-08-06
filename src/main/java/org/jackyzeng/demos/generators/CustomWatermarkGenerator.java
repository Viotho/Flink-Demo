package org.jackyzeng.demos.generators;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class CustomWatermarkGenerator implements WatermarkGenerator<Tuple2<String, Long>> {
    private final long maxOutOfOrderness = 60 * 1000;
    private long currentMaxTimestamp;

    @Override
    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(eventTimestamp, currentMaxTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness));
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Setting watermark emitting interval
        env.getConfig().setAutoWatermarkInterval(500L);
        DataStreamSource<Tuple2<String, Long>> input = env.fromElements(new Tuple2<>(), new Tuple2<>());

        // Custom watermark generator
        input.assignTimestampsAndWatermarks(
                WatermarkStrategy.forGenerator((context -> new CustomWatermarkGenerator()))
                        .withTimestampAssigner((event, timestamp) -> event.f1));

        input.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.f1));

        input.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.f1));
    }
}
