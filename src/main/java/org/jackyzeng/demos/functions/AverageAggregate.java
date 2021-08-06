package org.jackyzeng.demos.functions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.jackyzeng.demos.sources.StockSource;
import org.jackyzeng.demos.utils.StockPrice;

import java.time.Duration;

public class AverageAggregate implements AggregateFunction<StockPrice, Tuple3<String, Double, Integer>, Tuple2<String, Double>> {
    @Override
    public Tuple3<String, Double, Integer> createAccumulator() {
        return Tuple3.of("", 0d, 0);
    }

    @Override
    public Tuple3<String, Double, Integer> add(StockPrice stockPrice, Tuple3<String, Double, Integer> accumulator) {
        double price = accumulator.f1 + stockPrice.getPrice();
        int count = accumulator.f2 + 1;
        return Tuple3.of(stockPrice.getSymbol(), price, count);
    }

    @Override
    public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> accumulator) {
        return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2);
    }

    @Override
    public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> acc1, Tuple3<String, Double, Integer> acc2) {
        return Tuple3.of(acc1.f0, acc1.f1 + acc2.f1, acc1.f2 + acc2.f2);
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StockPrice> stockStream = env.addSource(new StockSource("/path/to/file"));
        stockStream.assignTimestampsAndWatermarks(WatermarkStrategy.<StockPrice>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()))
                .keyBy(StockPrice::getSymbol)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AverageAggregate());
    }
}
