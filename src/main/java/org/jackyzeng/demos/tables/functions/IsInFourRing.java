package org.jackyzeng.demos.tables.functions;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

// 标量函数（Scalar Function）接收零个、一个或者多个输入，输出一个单值标量。
public class IsInFourRing extends ScalarFunction {

    // 北京四环经纬度范围
    private static double LON_EAST = 116.48;
    private static double LON_WEST = 116.27;
    private static double LAT_NORTH = 39.988;
    private static double LAT_SOUTH = 39.83;

    // 判断输入的经纬度是否在四环内
    public boolean eval(double lon, double lat) {
        return !(lon > LON_EAST || lon < LON_WEST) &&
                !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }

    public boolean eval(float lon, float lat) {
        return !(lon > LON_EAST || lon < LON_WEST) &&
                !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }

    public boolean eval(String lonStr, String latStr) {
        double lon = Double.parseDouble(lonStr);
        double lat = Double.parseDouble(latStr);
        return !(lon > LON_EAST || lon < LON_WEST) &&
                !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2, configuration);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Tuple4<Long, Double, Double, Timestamp>> geoList = new ArrayList<>();
        geoList.add(Tuple4.of(1L, 116.2775, 39.91132, Timestamp.valueOf("2020-03-06 00:00:00")));
        geoList.add(Tuple4.of(2L, 116.44095, 39.88319, Timestamp.valueOf("2020-03-06 00:00:01")));
        geoList.add(Tuple4.of(3L, 116.25965, 39.90478, Timestamp.valueOf("2020-03-06 00:00:02")));
        geoList.add(Tuple4.of(4L, 116.27054, 39.87869, Timestamp.valueOf("2020-03-06 00:00:03")));

        DataStream<Tuple4<Long, Double, Double, Timestamp>> geoStream = env
                .fromCollection(geoList)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple4<Long, Double, Double, Timestamp>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f3.getTime())
                );

        Table geoTable = tEnv.fromDataStream(geoStream, $("id"), $("long"), $("alt"), $("ts").rowtime(), $("proc_ts").proctime());
        tEnv.createTemporaryView("geo", geoTable);
        // 注册函数到Catalog中，指定名字为IsInFourRing
        tEnv.createTemporarySystemFunction("IsInFourRing", new IsInFourRing());
        tEnv.sqlQuery("SELECT id FROM geo WHERE IsInFourRing(long, alt)");
        DataStream<Row> resultStream = tEnv.toAppendStream(geoTable, Row.class);
        resultStream.print();
        env.execute("Custom Table Function Demo");
    }
}