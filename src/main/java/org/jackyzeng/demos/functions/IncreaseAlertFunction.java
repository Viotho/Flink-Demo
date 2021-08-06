package org.jackyzeng.demos.functions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.jackyzeng.demos.sources.StockSource;
import org.jackyzeng.demos.utils.StockPrice;

import java.text.SimpleDateFormat;

public class IncreaseAlertFunction extends KeyedProcessFunction<String, StockPrice, String> {

    private long intervalMills;
    private ValueState<Double> lastPrice;
    private ValueState<Long> currentTimer;

    public IncreaseAlertFunction(long intervalMills) {
        this.intervalMills = intervalMills;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lastPrice = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastPrice", Types.DOUBLE));
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Types.LONG));
    }

    @Override
    public void processElement(StockPrice stockPrice, KeyedProcessFunction<String, StockPrice, String>.Context context, Collector<String> collector) throws Exception {
        if (lastPrice.value() == null) {
            // do nothing
        } else {
            Double prevPrice = lastPrice.value();
            long curTimerTimestamp;
            if (currentTimer.value() == null) {
                curTimerTimestamp = 0;
            } else {
                curTimerTimestamp = currentTimer.value();
            }
            if (stockPrice.getPrice() < prevPrice) {
                context.timerService().deleteEventTimeTimer(curTimerTimestamp);
                currentTimer.clear();
            } else if (stockPrice.getPrice() >= prevPrice && curTimerTimestamp == 0) {
                long timerTs = context.timestamp() + intervalMills;
                context.timerService().registerEventTimeTimer(timerTs);
                currentTimer.update(timerTs);
            }
        }
        lastPrice.update(stockPrice.getPrice());
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, StockPrice, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        out.collect(formatter.format(timestamp) + ", symbol: " + ctx.getCurrentKey() +
                " monotonically increased for " + intervalMills + " millisecond.");
        // 清空currentTimer状态
        currentTimer.clear();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StockPrice> inputStream = env.addSource(new StockSource("stock/stock-tick-20200108.csv"))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<StockPrice>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        SingleOutputStreamOperator<String> resultStream = inputStream.keyBy(StockPrice::getSymbol)
                .process(new IncreaseAlertFunction(3000));
        resultStream.print();
        env.execute("KeyedProcessFunction Increase Alert Demo");
    }
}
