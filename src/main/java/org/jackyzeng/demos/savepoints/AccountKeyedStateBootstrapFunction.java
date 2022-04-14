package org.jackyzeng.demos.savepoints;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.jackyzeng.demos.entities.Account;

public class AccountKeyedStateBootstrapFunction extends KeyedStateBootstrapFunction<Integer, Account> {

    private ValueState<Double> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("total", Types.DOUBLE);
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Account element, KeyedStateBootstrapFunction<Integer, Account>.Context context) throws Exception {
        state.update(element.getAmount());
    }
}
