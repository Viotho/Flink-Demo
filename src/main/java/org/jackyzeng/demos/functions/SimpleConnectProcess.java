package org.jackyzeng.demos.functions;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class SimpleConnectProcess {
    public static class SimpleCoMapFunction implements CoMapFunction<Integer, String, String> {

        @Override
        public String map1(Integer input1) throws Exception {
            return input1.toString();
        }

        @Override
        public String map2(String input2) throws Exception {
            return input2;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> intStream  = senv.fromElements(1, 0, 9, 2, 3, 6);
        DataStream<String> stringStream  = senv.fromElements("LOW", "HIGH", "LOW", "LOW");

        ConnectedStreams<Integer, String> connectedStream = intStream.connect(stringStream);
        DataStream<String> mapResult = connectedStream.map(new SimpleCoMapFunction());

        mapResult.print();
        senv.execute("Simple Connect Demo");
    }
}
