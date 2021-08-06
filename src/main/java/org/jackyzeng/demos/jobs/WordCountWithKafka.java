package org.jackyzeng.demos.jobs;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;


public class WordCountWithKafka {

	public static void main(String[] args) throws Exception {

//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Configuration configuration = new Configuration();
		configuration.setInteger(RestOptions.PORT, 8082);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, configuration);
//		env.setParallelism(2);

		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("group.id", "flink-group");
		String inputTopic = "KafkaTopic";
		String outputTopic = "WordCount";
		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(inputTopic, new SimpleStringSchema(), properties);
		DataStream<String> stream = env.addSource(consumer);

		DataStream<Tuple2<String, Integer>> wordCount = stream.flatMap((String line, Collector<Tuple2<String, Integer>> collectors) -> {
			String[] tokens = line.split("\\s");
			for (String token : tokens) {
				if (token.length() > 0) {
					collectors.collect(new Tuple2<>(token, 1));
				}
			}
		})
				.returns(Types.TUPLE(Types.STRING, Types.INT))
				.keyBy((KeySelector<Tuple2<String, Integer>, Object>) element -> element.f0)
				.window(TumblingEventTimeWindows.of(Time.seconds(5)))
				.sum(1);

		wordCount.print();
		env.execute("Kafka Streaming Word Count");
	}
}
