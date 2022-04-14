package org.jackyzeng.demos.savepoints;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.jackyzeng.demos.entities.Account;

import java.util.ArrayList;

public class SavepointWriting {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        int maxParallelism = 128;
        HashMapStateBackend stateBackend = new HashMapStateBackend();

        DataSource<Integer> integerDataSource = env.fromElements(1, 2, 3);
        ArrayList<Account> accounts = new ArrayList<>();
        DataSource<Account> accountDataSource = env.fromCollection(accounts);

        BootstrapTransformation<Integer> integerTransformation = OperatorTransformation.bootstrapWith(integerDataSource)
                .transform(new SimpleBootstrapFunction());

        BootstrapTransformation<Account> accountTransformation = OperatorTransformation.bootstrapWith(accountDataSource).keyBy((KeySelector<Account, Integer>) Account::getId)
                .transform(new AccountKeyedStateBootstrapFunction());

        Savepoint.create(stateBackend, maxParallelism)
                .withOperator("integer-operator", integerTransformation)
                .withOperator("account-operator", accountTransformation)
                .write("hdfs://path");

        env.execute("Savepoint Writing Demo");
    }
}
