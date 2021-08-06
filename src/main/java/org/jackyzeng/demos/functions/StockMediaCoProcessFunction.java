package org.jackyzeng.demos.functions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.jackyzeng.demos.sources.MediaSource;
import org.jackyzeng.demos.sources.StockSource;
import org.jackyzeng.demos.utils.Media;
import org.jackyzeng.demos.utils.StockPrice;

import java.util.Objects;

public class StockMediaCoProcessFunction extends KeyedCoProcessFunction<String, StockPrice, Media, StockPrice> {

    private ValueState<String> mediaState;

    @Override
    public void open(Configuration parameters) throws Exception {
        mediaState = getRuntimeContext().getState(new ValueStateDescriptor<String>("mediaStatusState", Types.STRING));
    }

    @Override
    public void processElement1(StockPrice stockPrice, KeyedCoProcessFunction<String, StockPrice, Media, StockPrice>.Context context, Collector<StockPrice> collector) throws Exception {
        String mediaStatus = this.mediaState.value();
        if (Objects.nonNull(mediaStatus)) {
            stockPrice.setMediaStatus(mediaStatus);
            collector.collect(stockPrice);
        }
    }

    @Override
    public void processElement2(Media media, KeyedCoProcessFunction<String, StockPrice, Media, StockPrice>.Context context, Collector<StockPrice> collector) throws Exception {
        mediaState.update(media.getStatus());
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StockPrice> stockStream = env
                .addSource(new StockSource("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<StockPrice>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        DataStream<Media> mediaStream = env
                .addSource(new MediaSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Media>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        SingleOutputStreamOperator<StockPrice> connectProcessedStream = stockStream.connect(mediaStream)
                .keyBy("symbol", "symbol")
                .process(new StockMediaCoProcessFunction());
        connectProcessedStream.print();
        env.execute("Connect Process Demo");
    }
}
