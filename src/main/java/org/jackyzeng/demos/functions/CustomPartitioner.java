package org.jackyzeng.demos.functions;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jackyzeng.demos.sources.StockSource;
import org.jackyzeng.demos.utils.StockPrice;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomPartitioner implements Partitioner<String> {
    private final Random rand = new Random();
    private final Pattern pattern = Pattern.compile(".*\\d+.*");

    @Override
    public int partition(String key, int numPartitions) {
        int randNum = rand.nextInt(numPartitions / 2);
        Matcher matcher = pattern.matcher(key);
        if (matcher.matches()) {
            return randNum;
        } else {
            return randNum + numPartitions / 2;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<StockPrice> stockStream = env.addSource(new StockSource("/path/to/file"));
        stockStream.partitionCustom(new CustomPartitioner(), StockPrice::getSymbol);
        stockStream.print();
        env.execute("Custom Partition");
    }
}
