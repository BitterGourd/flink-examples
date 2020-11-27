package com.github.streaming.json;

import com.github.streaming.json.util.TwitterExampleData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.StringTokenizer;

/**
 * 计算最常用的单词
 * 数据来源使用 {@link TwitterSource} (flink-connector-twitter_2.11)
 */
public class TwitterJson {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println("Usage: TwitterExample [--output <path>] " +
                "[--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token " +
                "<token> --twitter-source.tokenSecret <tokenSecret>]");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(params.getInt("parallelism", 1));

        DataStream<String> streamSource;
        if (params.has(TwitterSource.CONSUMER_KEY) &&
                params.has(TwitterSource.CONSUMER_SECRET) &&
                params.has(TwitterSource.TOKEN) &&
                params.has(TwitterSource.TOKEN_SECRET)) {
            streamSource = env.addSource(new TwitterSource(params.getProperties()));
        } else {
            System.out.println("Executing TwitterStream example with default props.");
            System.out.println("Use --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
                    "--twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret> specify the " +
                    "authentication info.");
            streamSource = env.fromElements(TwitterExampleData.TEXTS);
        }

        DataStream<Tuple2<String, Integer>> tweets = streamSource
                // (word, 1)
                .flatMap(new SelectEnglishAndTokenizeFlatMap())
                .keyBy(0)
                .sum(1);

        if (params.has("output")) {
            tweets.addSink(StreamingFileSink.forRowFormat(new Path(params.get("output")),
                    new SimpleStringEncoder<Tuple2<String, Integer>>()).build());
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            tweets.print();
        }

        env.execute("Flink Streaming Json Message");
    }

    private static class SelectEnglishAndTokenizeFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

        private static final long serialVersionUID = -2974684641303488980L;

        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }

            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user").get(
                    "lang").asText().equals("en");
            boolean hasText = jsonNode.has("text");

            if (isEnglish && hasText) {
                // StringTokenizer 是 Java 用于分隔字符串的工具类
                // message of tweet
                StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());

                // split the message
                while (tokenizer.hasMoreTokens()) {
                    String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();
                    if (!"".equals(result)) {
                        out.collect(new Tuple2<>(result, 1));
                    }
                }
            }
        }
    }
}
