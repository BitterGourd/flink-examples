package com.github.streaming.windowing;

import com.github.streaming.wordcount.WordCount;
import com.github.streaming.wordcount.util.WordCountData;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

/**
 * WordCount 窗口实现
 * <p>
 * 使用：--input path --output path --window n --slide n
 * 如果没有提供参数，则使用默认数据
 */
public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> text;
        if (params.has("input")) {
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("Executing WindowWordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");

            text = env.fromElements(WordCountData.WORDS);
        }

        env.getConfig().setGlobalJobParameters(params);

        final int windowSize = params.getInt("window", 10);
        final int slideSize = params.getInt("slide", 5);

        DataStream<Tuple2<String, Integer>> counts = text
                .flatMap(new WordCount.Tokenizer())
                .keyBy(0)

                /* event-time 滚动窗口 窗口的起止时间是 [0:00:00.000 - 0:59:59.999) */
                // .window(TumblingEventTimeWindows.of(Time.hours(1)))

                /* processing-time 滚动窗口 窗口起止时间是 [0:15:00.000 - 1:14:59.999) */
                // .window(TumblingProcessingTimeWindows.of(Time.hours(1), Time.seconds(15)))

                /* event-time 滑动窗口 */
                // .window(SlidingEventTimeWindows.of(Time.seconds(windowSize), Time.seconds(slideSize)))

                /* 数据驱动的窗口 */
                .countWindow(windowSize, slideSize)
                .sum(1);

        if (params.has("output")) {
            Path outPutPath = new Path(params.get("output"));
            counts.addSink(StreamingFileSink.forRowFormat(outPutPath,
                    new SimpleStringEncoder<Tuple2<String, Integer>>()).build());
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

        env.execute("Flink Streaming WindowWordCount");
    }
}
