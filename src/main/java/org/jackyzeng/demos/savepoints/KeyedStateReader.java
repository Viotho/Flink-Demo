package org.jackyzeng.demos.savepoints;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class KeyedStateReader extends KeyedStateReaderFunction<Integer, KeyedStateReader.KeyedState> {

    ValueState<Integer> state;
    ListState<Long> updateTimes;

    @Override
    public void open(Configuration configuration) throws Exception {
        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("state", Types.INT);
        state = getRuntimeContext().getState(valueStateDescriptor);
        ListStateDescriptor<Long> listStateDescriptor = new ListStateDescriptor<>("times", Types.LONG);
        updateTimes = getRuntimeContext().getListState(listStateDescriptor);
    }

    @Override
    public void readKey(Integer key, Context context, Collector<KeyedState> collector) throws Exception {
        KeyedState keyedState = new KeyedState();
        keyedState.key = key;
        keyedState.value = state.value();
        keyedState.times = StreamSupport.stream(updateTimes.get().spliterator(), false).collect(Collectors.toList());
        collector.collect(keyedState);
    }

    static class KeyedState {
        public int key;
        public int value;
        public List<Long> times;
    }
}

