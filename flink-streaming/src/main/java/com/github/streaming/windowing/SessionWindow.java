package com.github.streaming.windowing;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class SessionWindow {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);
        // 全局指定 event-time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        final List<Tuple3<String, Long, Integer>> input = generateInput();

        DataStream<Tuple3<String, Long, Integer>> source = env
                .addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
                        for (Tuple3<String, Long, Integer> value : input) {
                            // 发射数据时同时指定 timestamp（配合 TimeCharacteristic.EventTime 使用）
                            ctx.collectWithTimestamp(value, value.f1);
                            // TODO 用于数据乱序时，可以延迟数据接收
                            ctx.emitWatermark(new Watermark(value.f1 - 1));
                        }

                        ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
                    }

                    @Override
                    public void cancel() {

                    }
                });

        DataStream<Tuple3<String, Long, Integer>> aggregated = source
                .keyBy(0)
                // 超过 3 毫秒关闭 session-window
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
                .sum(2);

        aggregated.print();

        env.execute("Flink Streaming SessionWindowing");
    }

    private static List<Tuple3<String, Long, Integer>> generateInput() {
        final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();

        input.add(new Tuple3<>("a", 1L, 1));
        input.add(new Tuple3<>("b", 1L, 1));
        input.add(new Tuple3<>("b", 3L, 1));
        input.add(new Tuple3<>("b", 5L, 1));
        input.add(new Tuple3<>("c", 6L, 1));
        // We expect to detect the session "a" earlier than this point (the old
        // functionality can only detect here when the next starts)
        input.add(new Tuple3<>("a", 10L, 1));
        // We expect to detect session "b" and "c" at this point as well
        input.add(new Tuple3<>("c", 11L, 1));

        return input;
    }
}
