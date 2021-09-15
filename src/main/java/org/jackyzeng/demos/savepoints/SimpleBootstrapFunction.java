package org.jackyzeng.demos.savepoints;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.state.api.functions.StateBootstrapFunction;

public class SimpleBootstrapFunction extends StateBootstrapFunction<Integer> {
    private ListState<Integer> state;

    @Override
    public void processElement(Integer element, Context context) throws Exception {
        state.add(element);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("state", Types.INT);
        this.state = context.getOperatorStateStore().getListState(descriptor);
    }
}
