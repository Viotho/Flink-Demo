package org.jackyzeng.demos.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.jackyzeng.demos.utils.StockPrice;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FrequencyProcessWindowFunction extends ProcessWindowFunction<StockPrice, Tuple2<String, Double>, String, TimeWindow> {

    @Override
    public void process(String key, ProcessWindowFunction<StockPrice, Tuple2<String, Double>, String, TimeWindow>.Context context, Iterable<StockPrice> elements, Collector<Tuple2<String, Double>> collector) throws Exception {
        HashMap<Double, Integer> priceCountMap = new HashMap<>();
        for (StockPrice element : elements) {
            if (priceCountMap.containsKey(element.getPrice())) {
                Integer count = priceCountMap.get(element.getPrice());
                priceCountMap.put(element.getPrice(), count + 1);
            } else {
                priceCountMap.put(element.getPrice(), 1);
            }
        }

        List<Map.Entry<Double, Integer>> sortedList = priceCountMap.entrySet().stream().sorted((o1, o2) -> {
            if (o1.getValue() < o2.getValue()) {
                return 1;
            } else {
                return -1;
            }
        }).collect(Collectors.toList());

        if (sortedList.size() > 0) {
            collector.collect(Tuple2.of(key, sortedList.get(0).getKey()));
        }
    }
}
