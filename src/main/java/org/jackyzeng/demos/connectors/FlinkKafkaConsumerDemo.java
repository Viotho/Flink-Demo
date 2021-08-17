package org.jackyzeng.demos.connectors;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkKafkaConsumerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "flink-group");
        String topic = "topic";
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        consumer.setStartFromLatest();
        consumer.setStartFromGroupOffsets();
        DataStreamSource<String> kafkaStream = env.addSource(consumer);
        kafkaStream.print();
        env.execute("Kafka Consumer Demo");
    }
}
