package org.jackyzeng.demos.functions;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.jackyzeng.demos.sources.UserBehaviourSource;
import org.jackyzeng.demos.entities.BehaviourPattern;
import org.jackyzeng.demos.entities.UserBehaviour;

import java.util.Objects;

public class BroadcastStateDemo {
    public static class BroadcastPatternFunction extends KeyedBroadcastProcessFunction<Long, UserBehaviour, BehaviourPattern, Tuple2<Long, BehaviourPattern>> {

        private ValueState<String> lastBehaviourState;
        private MapStateDescriptor<Void, BehaviourPattern> bcPatternDesc;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastBehaviourState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-behaviour-state", Types.STRING));
            bcPatternDesc = new MapStateDescriptor<>("behaviour-pattern", Types.VOID, Types.POJO(BehaviourPattern.class));
        }

        @Override
        public void processElement(UserBehaviour userBehaviour, KeyedBroadcastProcessFunction<Long, UserBehaviour, BehaviourPattern, Tuple2<Long, BehaviourPattern>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<Long, BehaviourPattern>> collector) throws Exception {
            BehaviourPattern behaviourPattern = readOnlyContext.getBroadcastState(bcPatternDesc).get(null);
            String lastBehaviour = lastBehaviourState.value();
            if (Objects.nonNull(behaviourPattern) && Objects.nonNull(lastBehaviour)) {
                if (behaviourPattern.getFirstBehaviour().equals(lastBehaviour) &&
                        behaviourPattern.getSecondBehaviour().equals(userBehaviour.getBehaviour())) {
                    collector.collect(Tuple2.of(userBehaviour.getUserId(), behaviourPattern));
                }
            }
        }

        @Override
        public void processBroadcastElement(BehaviourPattern behaviourPattern, KeyedBroadcastProcessFunction<Long, UserBehaviour, BehaviourPattern, Tuple2<Long, BehaviourPattern>>.Context context, Collector<Tuple2<Long, BehaviourPattern>> collector) throws Exception {
            BroadcastState<Void, BehaviourPattern> bcPatternState = context.getBroadcastState(bcPatternDesc);
            bcPatternState.put(null, behaviourPattern);
        }

        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStreamSource<UserBehaviour> userBehaviourStream = env.addSource(new UserBehaviourSource("taobao/UserBehavior.json"));
            DataStreamSource<BehaviourPattern> behaviourPatternStream = env.fromElements(new BehaviourPattern());
            MapStateDescriptor<Void, BehaviourPattern> descriptor = new MapStateDescriptor<>(
                    "behaviour-pattern",
                    Types.VOID,
                    Types.POJO(BehaviourPattern.class));

            BroadcastStream<BehaviourPattern> broadcastStream = behaviourPatternStream.broadcast(descriptor);
            SingleOutputStreamOperator<Tuple2<Long, BehaviourPattern>> streamWithBroadcastState = userBehaviourStream.keyBy(UserBehaviour::getUserId)
                    .connect(broadcastStream)
                    .process(new BroadcastPatternFunction());

            streamWithBroadcastState.print();
            env.execute("BroadcastState Demo");
        }
    }
}
