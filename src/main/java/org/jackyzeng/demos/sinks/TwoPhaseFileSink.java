package org.jackyzeng.demos.sinks;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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
        transactionWriter.write(input.f0 + " " + input.f1 + "\n");
    }

    @Override
    protected String beginTransaction() throws Exception {
        String time = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        int subTaskInx = getRuntimeContext().getIndexOfThisSubtask();
        String fileName = time + "-" + subTaskInx;
        Path preCommitFilePath = Paths.get(preCommitPath + "/" + fileName);
        Files.createFile(preCommitFilePath);
        transactionWriter = Files.newBufferedWriter(preCommitFilePath);
        return fileName;
    }

    @Override
    protected void preCommit(String transaction) throws Exception {
        transactionWriter.flush();
        transactionWriter.close();
    }

    @Override
    protected void commit(String transaction) {
        Path preCommitFilePath = Paths.get(preCommitPath + "/" + transaction);
        if (Files.exists(preCommitFilePath)) {;
            Path committedPath = Paths.get(this.committedPath + "/" + transaction);
            try {
                Files.move(preCommitFilePath, committedPath);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    @Override
    protected void abort(String transaction) {
        Path preCommitFilePath = Paths.get(preCommitPath + "/" + transaction);
        if (Files.exists(preCommitFilePath)) {
            try {
                Files.delete(preCommitFilePath);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }
}
