package com.github.streaming.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 官方文档：https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/stream/operators/joining.html
 */
public class IntervalJoin {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Tuple3<String, Integer, Long>> greenStream = env.fromElements(
                new Tuple3<>("interval", 0, 1599534710000L),
                new Tuple3<>("interval", 1, 1599534711000L),
                new Tuple3<>("interval", 6, 1599534716000L),
                new Tuple3<>("interval", 7, 1599534717000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((element, recordTimestamp) -> element.f2));
        DataStream<Tuple3<String, Integer, Long>> orangeStream = env.fromElements(
                new Tuple3<>("interval", 0, 1599534710000L),
                new Tuple3<>("interval", 2, 1599534712000L),
                new Tuple3<>("interval", 3, 1599534713000L),
                new Tuple3<>("interval", 4, 1599534714000L),
                new Tuple3<>("interval", 5, 1599534715000L),
                new Tuple3<>("interval", 7, 1599534717000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((element, recordTimestamp) -> element.f2));

        orangeStream
                .keyBy(value -> value.f0)
                .intervalJoin(greenStream.keyBy(value -> value.f0))
                // orangeElem.ts + lowerBound <= greenElem.ts <= orangeElem.ts + upperBound
                .between(Time.seconds(-2), Time.seconds(1))
                .process(new ProcessJoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>,
                        String>() {
                    @Override
                    public void processElement(Tuple3<String, Integer, Long> left,
                                               Tuple3<String, Integer, Long> right, Context ctx,
                                               Collector<String> out) throws Exception {
                        out.collect(left.f1 + "," + right.f1);
                    }
                })
                .print();

        env.execute("Flink Streaming Interval Join.");
    }
}
