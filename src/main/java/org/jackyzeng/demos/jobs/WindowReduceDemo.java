package org.jackyzeng.demos.jobs;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.jackyzeng.demos.sources.StockSource;
import org.jackyzeng.demos.entities.StockPrice;

public class WindowReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StockPrice> stockStream = env.addSource(new StockSource("stock/stock-tick-20200108.csv"));
        SingleOutputStreamOperator<StockPrice> resultStream = stockStream
                .keyBy(StockPrice::getSymbol)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce((stockPrice1, stockPrice2) -> StockPrice.builder()
                        .symbol(stockPrice1.getSymbol())
                        .price(stockPrice2.getPrice())
                        .volume(stockPrice1.getVolume() + stockPrice2.getVolume())
                        .build());
        resultStream.print();
        env.execute("Window Demo");
    }
}

