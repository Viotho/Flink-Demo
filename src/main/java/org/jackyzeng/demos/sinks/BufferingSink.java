package org.jackyzeng.demos.sinks;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;

public class BufferingSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

    private final int threshold;
    private List<Tuple2<String, Integer>> bufferedElements;
    private transient ListState<Tuple2<String, Integer>> checkpointedState;

    public BufferingSink(int threshold, List<Tuple2<String, Integer>> bufferedElements) {
        this.threshold = threshold;
        this.bufferedElements = bufferedElements;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointedState.clear();
        for (Tuple2<String, Integer> bufferedElement : bufferedElements) {
            checkpointedState.add(bufferedElement);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor = new ListStateDescriptor<>(
                "buffered-elements",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        checkpointedState.clear();
        if (bufferedElements.size() == threshold) {
            // Output data to the outside system.
            checkpointedState.addAll(bufferedElements);
        }
    }
}
