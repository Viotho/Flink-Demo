package org.jackyzeng.demos.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class CustomCoGroupFunction implements CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String> {
    @Override
    public void coGroup(Iterable<Tuple2<String, Integer>> input1, Iterable<Tuple2<String, Integer>> input2, Collector<String> collector) throws Exception {
        input1.forEach(element -> System.out.println(element.f0 + ": " + element.f1));
        input2.forEach(element -> System.out.println(element.f0 + ": " + element.f1));
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> input1 = env.fromElements(Tuple2.of("One", 1), Tuple2.of("Two", 2));
        DataStreamSource<Tuple2<String, Integer>> input2 = env.fromElements(Tuple2.of("Three", 3), Tuple2.of("Four", 4));
        DataStream<String> coGroupedStream = input1.coGroup(input2)
                .where(element1 -> element1.f0)
                .equalTo(element2 -> element2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new CustomCoGroupFunction());

        coGroupedStream.print();
        env.execute("CoGroupFunction Demo");
    }
}
