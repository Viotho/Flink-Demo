package org.jackyzeng.demos.connectors;

import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class FlinkKafkaConsumerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-group");
        String topic = "topic";
        TypeInformationSerializationSchema<Tuple2<String, String>> serializationSchema = new TypeInformationSerializationSchema<>(TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }), env.getConfig());
//        JSONKeyValueDeserializationSchema jsonKeyValueDeserializationSchema = new JSONKeyValueDeserializationSchema(false);
        FlinkKafkaConsumer<Tuple2<String, String>> consumer = new FlinkKafkaConsumer<>(topic, serializationSchema, properties);
//        consumer.setStartFromEarliest();
//        consumer.setStartFromLatest();
        consumer.setStartFromGroupOffsets();
        DataStreamSource<Tuple2<String, String>> kafkaStream = env.addSource(consumer);
        kafkaStream.print();
        env.execute("Kafka Consumer Demo");
    }
}
