package org.jackyzeng.demos.sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SimpleSource implements SourceFunction<Tuple2<String, Integer>> {
    private int offset = 0;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
        while (isRunning) {
            Thread.sleep(1000);
            sourceContext.collect(Tuple2.of("" + offset, offset));
            offset++;
            if (offset == 1000) {
                isRunning = false;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
