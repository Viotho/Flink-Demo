package org.jackyzeng.demos.functions.triggers;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.jackyzeng.demos.functions.AverageAggregate;
import org.jackyzeng.demos.sources.StockSource;
import org.jackyzeng.demos.entities.StockPrice;

import java.util.Objects;

public class CustomTrigger extends Trigger<StockPrice, TimeWindow> {
    @Override
    public TriggerResult onElement(StockPrice stockPrice, long time, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        ValueState<Double> lastPriceState = triggerContext.getPartitionedState(new ValueStateDescriptor<Double>("lastPriceState", Types.DOUBLE));
        TriggerResult triggerResult = TriggerResult.CONTINUE;
        if (Objects.nonNull(lastPriceState.value())) {
            if (lastPriceState.value() - stockPrice.getPrice() > lastPriceState.value() * 0.05) {
                triggerResult = TriggerResult.FIRE_AND_PURGE;
            } else if((lastPriceState.value() - stockPrice.getPrice() > lastPriceState.value() * 0.01)) {
                long t = triggerContext.getCurrentProcessingTime() + (10 * 1000 - (triggerContext.getCurrentProcessingTime() % 10 * 1000));
                triggerContext.registerProcessingTimeTimer(t);
            }
        }
        lastPriceState.update(stockPrice.getPrice());
        return triggerResult;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        ValueState<Double> lastPriceState = triggerContext.getPartitionedState(new ValueStateDescriptor<Double>("lastPriceState", Types.DOUBLE));
        lastPriceState.clear();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StockPrice> stockStream = env.addSource(new StockSource("stock/stock-tick-20200108.csv"));
        SingleOutputStreamOperator<Tuple2<String, Double>> average = stockStream.keyBy(StockPrice::getSymbol)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .trigger(new CustomTrigger())
                .aggregate(new AverageAggregate());

        average.print();
        env.execute("Trigger Demo");
    }
}
