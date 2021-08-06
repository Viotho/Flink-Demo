package org.jackyzeng.demos.functions;

import org.apache.flink.api.common.functions.Partitioner;

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
}
