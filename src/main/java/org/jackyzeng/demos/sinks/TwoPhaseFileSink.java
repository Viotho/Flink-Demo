package org.jackyzeng.demos.sinks;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.jackyzeng.demos.functions.FailingMapper;
import org.jackyzeng.demos.sources.CheckpointedSource;

import java.io.BufferedWriter;
import java.io.IOException;
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

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(5 * 1000);
        DataStreamSource<Tuple2<String, Integer>> countStream = env.addSource(new CheckpointedSource());
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = countStream.map(new FailingMapper(20));
        String preCommitPath = "/tmp/flink-sink-precommit";
        String committedPath = "tmp/flink-sink-committed";
        if (!Files.exists(Paths.get(preCommitPath))) {
            Files.createDirectory(Paths.get(preCommitPath));
        }
        if (!Files.exists(Paths.get(committedPath))) {
            Files.createDirectory(Paths.get(committedPath));
        }
        result.addSink(new TwoPhaseFileSink(preCommitPath, committedPath));
        env.execute("Two Phase Commit Sink Demo");
    }
}
