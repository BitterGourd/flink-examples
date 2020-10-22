package com.github.streaming.join;

import com.github.streaming.join.util.WindowJoinSampleData;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 官方文档：https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/stream/operators/joining.html
 *
 * 窗口连接 (name, grade) / (name, salary)
 */
public class WindowJoin {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final long windowSize = params.getLong("windowSize", 2000L);
        final long rate = params.getLong("rate", 3L);

        System.out.println("Using windowSize=" + windowSize + ", data rate=" + rate);
        System.out.println("To customize example, use: WindowJoin [--windowSize <window-size-in-millis>] [--rate " +
                "<elements-per-second>]");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStream<Tuple2<String, Integer>> grades = WindowJoinSampleData.GradeSource.getSource(env, rate);
        DataStream<Tuple2<String, Integer>> salaries = WindowJoinSampleData.SalarySource.getSource(env, rate);

        grades.join(salaries)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())
                // 滚动窗口
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                // 滑动窗口
                // .window(SlidingEventTimeWindows.of(Time.milliseconds(2) /* size */, Time.milliseconds(1) /* slide */))
                // 会话窗口
                // .window(EventTimeSessionWindows.withGap(Time.milliseconds(1)))
                // 指定联接规则 - 针对窗口中相同 key 进行联接
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer,
                        Integer>>() {
                    private static final long serialVersionUID = -1093873017929616092L;

                    @Override
                    public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> first, Tuple2<String,
                            Integer> second) throws Exception {
                        return new Tuple3<>(first.f0, first.f1, second.f1);
                    }
                })
                .print();

        env.execute("Flink Streaming WindowJoin");
    }

    private static class NameKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        private static final long serialVersionUID = -2337347436101827969L;

        @Override
        public String getKey(Tuple2<String, Integer> value) throws Exception {
            return value.f0;
        }
    }
}
