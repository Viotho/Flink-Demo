package org.jackyzeng.demos.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.jackyzeng.demos.sources.StockSource;
import org.jackyzeng.demos.entities.StockPrice;

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

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StockPrice> stockStream = env.addSource(new StockSource("stock/stock-tick-20200108.csv"));
        SingleOutputStreamOperator<Tuple2<String, Double>> average = stockStream.keyBy(StockPrice::getSymbol)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AverageAggregate());
        average.print();
        env.execute("Aggregate Function Demo");
    }
}
