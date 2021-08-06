package org.jackyzeng.demos.functions;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        DataStreamSource<Tuple2<Integer, String>> inputStream = env.fromElements(Tuple2.of(1, "123"), Tuple2.of(2, "abc"),
                Tuple2.of(3, "256"), Tuple2.of(4, "zyx"),
                Tuple2.of(5, "bcd"), Tuple2.of(6, "666"));
        inputStream.partitionCustom(new CustomPartitioner(), tuple2 -> tuple2.f1);
        inputStream.print();
        env.execute("Custom Partition Demo");
    }
}
