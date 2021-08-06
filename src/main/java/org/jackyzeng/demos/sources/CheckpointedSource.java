package org.jackyzeng.demos.sources;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public class CheckpointedSource extends RichSourceFunction<Tuple2<String, Integer>> implements CheckpointedFunction {

    private int offset;
    private boolean isRunning = true;
    private ListState<Integer> offsetState;

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        offsetState.clear();
        offsetState.add(offset);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("offset", Types.INT);
        offsetState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);
        Iterable<Integer> iter = offsetState.get();
        if (iter == null || !iter.iterator().hasNext()){
            offset = 0;
        } else {
            offset = iter.iterator().next();
        }
    }

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {

        while (isRunning) {
            Thread.sleep(100);
            synchronized (sourceContext.getCheckpointLock()) {
//                sourceContext.collect(new Tuple2<>("" + offset, 1));
                sourceContext.collectWithTimestamp(new Tuple2<>("" + offset, offset), System.currentTimeMillis());
                offset++;
            }
            if (offset == 100) {
                sourceContext.emitWatermark(new Watermark(System.currentTimeMillis()));
            }

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
