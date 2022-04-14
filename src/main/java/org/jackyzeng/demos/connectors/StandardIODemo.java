package org.jackyzeng.demos.connectors;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.jackyzeng.demos.entities.StockPrice;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class StandardIODemo {

    private static final String hostName = "host";
    private static final String filePath = "path/to/file";
    private static final int port = 8080;

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Check a class whether it is POJO type.
        // If using KryoSerializer, then it is not POJO type.
        System.out.println(TypeInformation.of(StockPrice.class).createSerializer(new ExecutionConfig()));

        // Disable Kryo type.
        env.getConfig().disableGenericTypes();

        // Socket Text Stream.
        DataStreamSource<String> socketTextStream = env.socketTextStream(hostName, port, "\n");
        socketTextStream.writeToSocket(hostName, port, new SimpleStringSchema());

        // Collections and standard output.
        DataStreamSource<Integer> integerStream = env.fromElements(1, 2, 3);
        Object[] data = new Object[1];
        data[0] = new Object();
        TypeInformation<Object> typeInformation = TypeInformation.of(Object.class);
        DataStreamSource<Object> objectStream = env.fromCollection(Arrays.asList(data), typeInformation);
        objectStream.print();
        objectStream.printToErr();

        // File System.
        DataStreamSource<String> fileStream = env.readTextFile(filePath);
        // Read file with FileInputFormat.
        TextInputFormat textInputFormat = new TextInputFormat(new Path(filePath));
        DataStreamSource<String> file = env.readFile(textInputFormat, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100);
        StreamingFileSink<String> fileSink = StreamingFileSink.forRowFormat(new Path("path/to/file"), new SimpleStringEncoder<String>("UTF-8")).build();
        file.addSink(fileSink);
    }

    static class Tuple2Encoder implements Encoder<Tuple2<String, Integer>> {
        @Override
        public void encode(Tuple2<String, Integer> element, OutputStream outputStream) throws IOException {
            outputStream.write((element.f0 + "@" + element.f1).getBytes(StandardCharsets.UTF_8));
            outputStream.write('\n');
        }
    }
}
