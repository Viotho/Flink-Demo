package org.jackyzeng.demos.sinks;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.io.BufferedWriter;

public class TwoPhaseFileSink extends TwoPhaseCommitSinkFunction<Tuple2<String, Integer>, String, Void> {

    private String preCommitPath;
    private String committedPath;
    private BufferedWriter transactionWriter;

    public TwoPhaseFileSink(String preCommitPath, String committedPath) {
        super(StringSerializer.INSTANCE, VoidSerializer.INSTANCE);
        this.preCommitPath = preCommitPath;
        this.committedPath = committedPath;
    }

    @Override
    protected void invoke(String transaction, Tuple2<String, Integer> input, Context context) throws Exception {
        
    }

    @Override
    protected String beginTransaction() throws Exception {
        return null;
    }

    @Override
    protected void preCommit(String transaction) throws Exception {

    }

    @Override
    protected void commit(String transaction) {

    }

    @Override
    protected void abort(String transaction) {

    }
}
