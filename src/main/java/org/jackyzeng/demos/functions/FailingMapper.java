package org.jackyzeng.demos.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public  class FailingMapper implements MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

    private int count = 0;
    private int failInterval;

    public FailingMapper(int failInterval) {
        this.failInterval = failInterval;
    }

    @Override
    public Tuple2<String, Integer> map(Tuple2<String, Integer> input) {
        count += 1;
        if (count > failInterval) {
            throw new RuntimeException("job fail! show how flink checkpoint and recovery");
        }
        return input;
    }
}
