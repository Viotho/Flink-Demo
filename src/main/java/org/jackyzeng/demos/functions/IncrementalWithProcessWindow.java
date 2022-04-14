package org.jackyzeng.demos.functions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.jackyzeng.demos.sources.StockSource;
import org.jackyzeng.demos.entities.StockPrice;

public class IncrementalWithProcessWindow {
    public static class MaxMinReduce implements ReduceFunction<Tuple4<String, Double, Double, Long>> {

        @Override
        public Tuple4<String, Double, Double, Long> reduce(Tuple4<String, Double, Double, Long> element1, Tuple4<String, Double, Double, Long> element2) throws Exception {
            return Tuple4.of(element1.f0, Math.max(element1.f1, element2.f1), Math.min(element1.f2, element2.f2), 0L);
        }
    }

    public static class WindowEndProcessFunction extends ProcessWindowFunction<Tuple4<String, Double, Double, Long>, Tuple4<String, Double, Double, Long>, String, TimeWindow> {

        @Override
        public void process(String key, ProcessWindowFunction<Tuple4<String, Double, Double, Long>, Tuple4<String, Double, Double, Long>, String, TimeWindow>.Context context, Iterable<Tuple4<String, Double, Double, Long>> elements, Collector<Tuple4<String, Double, Double, Long>> collector) throws Exception {
            long windowEndTs = context.window().getEnd();
            if (elements.iterator().hasNext()) {
                Tuple4<String, Double, Double, Long> firstElement = elements.iterator().next();
                collector.collect(Tuple4.of(key, firstElement.f1, firstElement.f2, windowEndTs));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StockPrice> stockStream = env
                .addSource(new StockSource("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<StockPrice>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        SingleOutputStreamOperator<Tuple4<String, Double, Double, Long>> resultStream = stockStream.map(stockPrice -> Tuple4.of(stockPrice.getSymbol(), stockPrice.getPrice(), stockPrice.getPrice(), 0L))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.LONG))
                .keyBy(element -> element.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new MaxMinReduce(), new WindowEndProcessFunction());

        resultStream.print();
        env.execute("Incremental With ProcessWindow Demo");
    }
}
