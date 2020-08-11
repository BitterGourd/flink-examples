package com.github.streaming.wordcount;

import com.github.streaming.wordcount.util.WordCountData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

public class WordCount {
    public static void main(String[] args) throws Exception {
        /*
         * 解析运行参数，参数 key 以 - 或 -- 开头
         * 示例：--key1 value1 --key2 value2 -key3 value3
         *      --multi multiValue1 --multi multiValue2
         */
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使参数在 Web 界面中可用
        env.getConfig().setGlobalJobParameters(params);

        // 输入数据
        DataStream<String> text = null;
        if (params.has("input")) {
            // 合并所有输入文件数据
            for (String input : params.getMultiParameter("input")) {
                if (text == null) {
                    text = env.readTextFile(input);
                } else {
                    text = text.union(env.readTextFile(input));
                }
            }

            // flink 内置检查工具
            Preconditions.checkNotNull(text, "Input DataStream should not be null.");
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");

            text = env.fromElements(WordCountData.WORDS);
        }

        SingleOutputStreamOperator<Tuple2<String, Integer>> counts = text
                .flatMap(new Tokenizer())
                // 按元组第一个元素分组，即单词
                .keyBy(0)
                // 按元组第二个元素求和，即出现次数
                .sum(1);

        if (params.has("output")) {
            Path outPutPath = new Path(params.get("output"));
            counts.addSink(StreamingFileSink.forRowFormat(outPutPath,
                    new SimpleStringEncoder<Tuple2<String, Integer>>()).build())
                    // 控制输出为一个文件
                    .setParallelism(1);
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

        env.execute("Flink Streaming WordCount");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        private static final long serialVersionUID = -2565129499040481510L;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // \W 用于匹配任何非单词字符，等价于 [^A-Za-z0-9_]
            String[] tokens = value.toLowerCase().split("\\W+");
            // 展开成二元组
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(Tuple2.of(token, 1));
                }
            }
        }
    }
}
