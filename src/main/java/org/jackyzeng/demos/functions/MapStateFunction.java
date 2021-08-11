package org.jackyzeng.demos.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.jackyzeng.demos.utils.UserBehaviour;

public class MapStateFunction extends RichFlatMapFunction<UserBehaviour, Tuple3<Long, String, Integer>> {

    private MapState<String, Integer> behaviourMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Integer> behaviourMapStateDescriptor = new MapStateDescriptor<>("behaviourMap", Types.STRING, Types.INT);
        behaviourMapState = getRuntimeContext().getMapState(behaviourMapStateDescriptor);
    }

    @Override
    public void flatMap(UserBehaviour input, Collector<Tuple3<Long, String, Integer>> collector) throws Exception {
        int behaviourCnt = 1;
        if (behaviourMapState.contains(input.getBehaviour())) {
            behaviourCnt = behaviourMapState.get(input.getBehaviour() + 1);
        }
        behaviourMapState.put(input.getBehaviour(), behaviourCnt);
        collector.collect(Tuple3.of(input.getUserId(), input.getBehaviour(), behaviourCnt));
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<UserBehaviour> userBehaviourStream = env.fromElements(new UserBehaviour(), new UserBehaviour(), new UserBehaviour());
        KeyedStream<UserBehaviour, Long> keyedStream = userBehaviourStream.keyBy(user -> user.getUserId());
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> behaviourCountStream = keyedStream.flatMap(new MapStateFunction());
        behaviourCountStream.print();
        env.execute("Rich Map Function State Demo");
    }

    private static class ReducingStateFlatMap extends RichFlatMapFunction<Tuple2<Integer, Integer>, Integer> {

        private transient ReducingState<Integer> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            ReducingStateDescriptor<Integer> stateDescriptor = new ReducingStateDescriptor<>("reducing-state", new ReducingSum(), Types.INT);
            this.state = getRuntimeContext().getReducingState(stateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<Integer, Integer> value, Collector<Integer> collector) throws Exception {
            state.add(value.f1);
        }

        private static  class ReducingSum implements ReduceFunction<Integer> {

            @Override
            public Integer reduce(Integer value1, Integer value2) throws Exception {
                return value1 + value2;
            }
        }
    }
}
