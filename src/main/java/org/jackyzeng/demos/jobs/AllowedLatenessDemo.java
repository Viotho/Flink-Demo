package org.jackyzeng.demos.jobs;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class AllowedLatenessDemo {
    public static class AllowedLatenessFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, Tuple4<String, String, Integer, String>, String, TimeWindow> {
        @Override
        public void process(String key, ProcessWindowFunction<Tuple3<String, Long, Integer>, Tuple4<String, String, Integer, String>, String, TimeWindow>.Context context, Iterable<Tuple3<String, Long, Integer>> elements, Collector<Tuple4<String, String, Integer, String>> collector) throws Exception {
            ValueState<Boolean> isUpdated = context.windowState().getState(new ValueStateDescriptor<Boolean>("isUpdated", Types.BOOLEAN));
            int count = 0;
            for (Object element : elements) {
                count++;
            }

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            if (isUpdated.value() == null || !isUpdated.value()) {
                collector.collect(Tuple4.of(key, format.format(Calendar.getInstance().getTime()), count, "first"));
                isUpdated.update(true);
            } else {
                collector.collect(Tuple4.of(key, format.format(Calendar.getInstance().getTime()), count, "updated"));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, Long, Integer>> input = env.fromElements(
                Tuple3.of("One",System.currentTimeMillis(), 1),
                Tuple3.of("Two", System.currentTimeMillis() ,2));

        SingleOutputStreamOperator<Tuple4<String, String, Integer, String>> allowedLatenessStream = input.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((element, timestamp) -> element.f1))
                .keyBy(element -> element.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5))
                .process(new AllowedLatenessFunction());

        allowedLatenessStream.print();
        env.execute("Allowed Lateness Demo");
    }
}
