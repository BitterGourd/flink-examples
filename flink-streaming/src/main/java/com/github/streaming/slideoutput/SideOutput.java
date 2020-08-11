package com.github.streaming.slideoutput;

import com.github.streaming.windowing.WindowWordCount;
import com.github.streaming.wordcount.util.WordCountData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * {@link WindowWordCount} 的修改版本，使用侧输出流(SlideOutput)进行分流
 * 字符长度大于 5 的单词输出到侧输出流，大于 0 小于等于 5 的单词进行计数统计
 */
public class SideOutput {

    @SuppressWarnings("serial")
    private static final OutputTag<String> rejectedWordsTag = new OutputTag<String>("rejected") {
    };

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);
        // 设置时间语义为事件进入 Flink 的时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStream<String> text;
        if (params.has("input")) {
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");

            text = env.fromElements(WordCountData.WORDS);
        }

        SingleOutputStreamOperator<Tuple2<String, Integer>> tokenized = text
                .keyBy(new KeySelector<String, Integer>() {
                    private static final long serialVersionUID = 4413554759715302774L;

                    @Override
                    public Integer getKey(String value) throws Exception {
                        return 0;
                    }
                })
                .process(new Tokenizer());

        DataStream<String> rejectedWords = tokenized
                .getSideOutput(rejectedWordsTag)
                .map(new MapFunction<String, String>() {
                    private static final long serialVersionUID = 2542173128976071779L;

                    @Override
                    public String map(String value) throws Exception {
                        return "rejected : " + value;
                    }
                });

        DataStream<Tuple2<String, Integer>> counts = tokenized
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1)))
                .sum(1);

        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
            rejectedWords.writeAsText(params.get("rejected-words-output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
            rejectedWords.print();
        }

        env.execute("Flink Streaming WordCount SlideOutput");
    }

    @SuppressWarnings("serial")
    public static final class Tokenizer extends KeyedProcessFunction<Integer, String, Tuple2<String, Integer>> {

        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");

            for (String token : tokens) {
                if (token.length() > 5) {
                    ctx.output(rejectedWordsTag, token);
                } else if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
