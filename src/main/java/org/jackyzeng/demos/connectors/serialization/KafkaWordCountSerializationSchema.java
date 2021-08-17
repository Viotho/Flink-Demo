package org.jackyzeng.demos.connectors.serialization;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class KafkaWordCountSerializationSchema implements KafkaSerializationSchema<Tuple2<String, Integer>> {

    private String topic;

    public KafkaWordCountSerializationSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Integer> element, @Nullable Long timestamp) {
        return new ProducerRecord<>(topic, (element.f0 + ": " + element.f1).getBytes(StandardCharsets.UTF_8));
    }
}
