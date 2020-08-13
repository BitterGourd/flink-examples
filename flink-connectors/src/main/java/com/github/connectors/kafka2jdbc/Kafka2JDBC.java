package com.github.connectors.kafka2jdbc;

import com.github.connectors.pojo.LogEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Source : Kafka
 * Sink : JDBC (ClickHouse)
 * Kafka Event : JSON 格式的日志，一定包含 lt 和 timestamp 字段
 *      {"lt": "appboot", "timestamp": 1597216980000, "user_id": 1}
 *
 * 将日志中的 lt 和 timestamp 取出，并将整个 JSON 串作为一个字段存入数据库
 */
public class Kafka2JDBC {

    public static void main(String[] args) throws Exception {

        printUsage();

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(2000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        final String stateBackend = params.get("backend", "memory");
        if ("file".equals(stateBackend)) {
            final String checkpointDir = params.get("checkpoint-dir");
            boolean asyncCheckpoints = params.getBoolean("async-checkpoints", false);
            env.setStateBackend(new FsStateBackend(checkpointDir, asyncCheckpoints));
        } else if ("rocks".equals(stateBackend)) {
            final String checkpointDir = params.get("checkpoint-dir");
            boolean incrementalCheckpoints = params.getBoolean("incremental-checkpoints", false);
            env.setStateBackend(new RocksDBStateBackend(checkpointDir, incrementalCheckpoints));
        }

        DataStreamSource<String> streamSource = env.addSource(generateKafkaSource(params));

        DataStream<LogEvent> transformedStream = streamSource.map(new MapFunction<String, LogEvent>() {
            private static final long serialVersionUID = 1315720994164942082L;

            private transient ObjectMapper jsonParser;

            @Override
            public LogEvent map(String value) throws Exception {
                if (jsonParser == null) {
                    jsonParser = new ObjectMapper();
                }

                JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
                String lt = jsonNode.get("lt").asText();
                LocalDateTime time = LocalDateTime.ofEpochSecond(jsonNode.get("timestamp").asLong() / 1000,
                        0, ZoneOffset.ofHours(8));

                return new LogEvent(lt, time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), value);
            }
        });

        transformedStream.print();
        transformedStream.addSink(generateJDBCSink(params));

        env.execute("Flink Streaming Kafka2JDBC");
    }

    private static SinkFunction<LogEvent> generateJDBCSink(ParameterTool params) {
        return JdbcSink.sink(
                "INSERT INTO logs VALUES (?,?,?)",
                (ps, t) -> {
                    ps.setString(1, t.getLt());
                    ps.setString(2, t.getTimestamp());
                    ps.setString(3, t.getEvent());
                },
                JdbcExecutionOptions.builder().withBatchIntervalMs(3000L).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(params.get("jdbc-driver"))
                        .withUrl(params.get("jdbc-url"))
                        .withUsername(params.get("jdbc-username"))
                        .build()
        );
    }

    private static FlinkKafkaConsumer<String> generateKafkaSource(ParameterTool params) {
        String kafkaTopic = params.get("kafka-topic");
        String brokers = params.get("brokers", "localhost:9092");

        System.out.format("Reading from kafka topic %s @ %s%n%n", kafkaTopic, brokers);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", brokers);

        FlinkKafkaConsumer<String> kafka = new FlinkKafkaConsumer<String>(kafkaTopic, new SimpleStringSchema(),
                kafkaProps);
        // 指定 Kafka 消费 offset 策略，如果已保存 checkpoint 或 savepoint，则此项配置不生效，以状态存储为准
        kafka.setStartFromLatest();
        // 是否自动提交 Offset，仅在开启 checkpoint 时有效
        // 否则应该使用 "auto.commit.enable" (for 0.8) / "enable.auto.commit" (for 0.9+)
        kafka.setCommitOffsetsOnCheckpoints(false);

        return kafka;
    }

    private static void printUsage() {
        System.out.println("Usage with Kafka: Kafka2JDBC --kafka-topic <topic> [--brokers <brokers>]");
        System.out.println("Usage with JDBC: Kafka2JDBC --jdbc-driver <driver> --jdbc-username <username> " +
                "[--jdbc-password <password>] [--jdbc-url <url>]");
        System.out.println("Options for both the above setups: ");
        System.out.println("\t[--backend <file|rocks>]");
        System.out.println("\t[--checkpoint-dir <filepath>]");
        System.out.println("\t[--async-checkpoints <true|false>]");
        System.out.println("\t[--incremental-checkpoints <true|false>]");
    }
}
