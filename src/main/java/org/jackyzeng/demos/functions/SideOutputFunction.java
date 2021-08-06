package org.jackyzeng.demos.functions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.jackyzeng.demos.sources.StockSource;
import org.jackyzeng.demos.utils.StockPrice;

public class SideOutputFunction extends KeyedProcessFunction<String, StockPrice, String> {

    private static final OutputTag<StockPrice> highVolumeOutput = new OutputTag<StockPrice>("high-volume-trade"){};

    @Override
    public void processElement(StockPrice stockPrice, KeyedProcessFunction<String, StockPrice, String>.Context context, Collector<String> collector) throws Exception {
        if (stockPrice.getPrice() > 100) {
            context.output(highVolumeOutput, stockPrice);
        } else {
            collector.collect("normal data");
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StockPrice> input = env.addSource(new StockSource("/path/to/file"));
        SingleOutputStreamOperator<String> mainStream = input
                .keyBy(StockPrice::getSymbol)
                .process(new SideOutputFunction());

        DataStream<StockPrice> sideOutputStream = mainStream.getSideOutput(highVolumeOutput);
        mainStream.print();
        sideOutputStream.print();
        env.execute("SideOutput Demo");
    }

}
