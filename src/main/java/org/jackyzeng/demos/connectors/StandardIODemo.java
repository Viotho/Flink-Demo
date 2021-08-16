package org.jackyzeng.demos.connectors;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class StandardIODemo {

    private static final String hostName = "host";
    private static final int port = 8080;

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

    }
}
