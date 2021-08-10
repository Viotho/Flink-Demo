package org.jackyzeng.demos.functions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoinDemo {
    public static class CustomJoinFunction extends ProcessJoinFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, String> {
        @Override
        public void processElement(Tuple3<String, Long, Integer> input1, Tuple3<String, Long, Integer> input2, ProcessJoinFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, String>.Context context, Collector<String> collector) throws Exception {
            collector.collect(input1.toString() + " " + input2.toString());
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, Long, Integer>> input1 = env.fromElements(Tuple3.of("One",System.currentTimeMillis(), 1), Tuple3.of("Two", System.currentTimeMillis() ,2));
        DataStreamSource<Tuple3<String, Long, Integer>> input2 = env.fromElements(Tuple3.of("Three", System.currentTimeMillis(), 3), Tuple3.of("Four", System.currentTimeMillis(), 4));

        SingleOutputStreamOperator<String> intervalJoinStream = input1.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((element, timestamp) -> element.f1))
                .keyBy(element1 -> element1.f0)
                .intervalJoin(input2.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((element, timestamp) -> element.f1))
                        .keyBy(element2 -> element2.f0))
                .between(Time.milliseconds(-5), Time.milliseconds(10))
                .process(new CustomJoinFunction());

        intervalJoinStream.print();
        env.execute("Interval Join Demo");
    }
}
