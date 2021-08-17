package org.jackyzeng.demos.connectors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.jackyzeng.demos.connectors.serialization.KafkaWordCountSerializationSchema;

import java.util.Properties;

public class FlinkKafkaProducerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> wordCount = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("world", 1));
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        String topic = "topic";
        FlinkKafkaProducer<Tuple2<String, Integer>> producer = new FlinkKafkaProducer<>(topic, new KafkaWordCountSerializationSchema(topic),
                properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        wordCount.addSink(producer);
        env.execute("Flink Kafka Producer Demo");
    }
}
