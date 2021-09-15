package org.jackyzeng.demos.savepoints;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;

public class SavepointReading {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StateBackend stateBackend = new HashMapStateBackend();
        ExistingSavepoint savepoint = Savepoint.load(env, "hdfs://path/", stateBackend);
        DataSet<Integer> listState = savepoint.readListState("source-id", "state-name", Types.INT);
        DataSet<Integer> unionState = savepoint.readUnionState("union-source-id", "union-state-name", Types.INT);
        DataSource<KeyedStateReader.KeyedState> keyedState = savepoint.readKeyedState("mapper-id", new KeyedStateReader());
        env.execute("Savepoint Reading");
    }
}
