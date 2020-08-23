package com.github.connectors.kafka;

import com.github.connectors.pojo.Log;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

import java.util.Properties;

/**
 * flink-connector-kafka 官方文档：https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/connectors/kafka.html
 *
 * 官方建议版本适配：
 *  flink-connector-kafka-0.10_2.11 =>  Kafka 0.10.x
 *  flink-connector-kafka-0.11_2.11 =>  Kafka 0.11.x
 *  flink-connector-kafka_2.11      =>  Kafka >= 1.0.0
 *
 * Flink Kafka Consumer 集成了 Flink 的 Checkpoint 机制，可提供 exactly-once 的处理语义
 * 为此，Flink 并不完全依赖于跟踪 Kafka 消费组的偏移量，而是在内部跟踪和检查偏移量
 */
public class Kafka2Kafka {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        if (params.has("checkpoint-dir")) {
            env.setStateBackend(new FsStateBackend(params.get("checkpoint-dir")));
        }


        // ==============================================================
        //                  Flink Kafka Consumer
        // ==============================================================
        String kafkaTopic = params.get("consumer-kafka-topic", "test");
        String brokers = params.get("consumer-brokers", "localhost:9092");
        String groupId = params.get("consumer-group-id", "test");
        System.out.format("Reading from kafka topic %s @ %s with group id [%s]%n%n", kafkaTopic, brokers, groupId);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", brokers);
        kafkaProps.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> kafka = new FlinkKafkaConsumer<String>(kafkaTopic, new SimpleStringSchema(),
                kafkaProps);
        // ------------------------------------------------------------------------------------------------
        // 指定 Kafka 消费 offset 策略：从最新的记录开始
        // 如果程序从 checkpoint 或 savepoint 恢复，此方法不影响从何处读取，只会使用 checkpoint 或 savepoint 中的 offset
        kafka.setStartFromLatest();
        // 尽可能从最早的记录开始
        // kafka.setStartFromEarliest();
        // 从指定的时间开始（毫秒）
        // kafka.setStartFromTimestamp(1597980113000L);
        // 默认的方法。从 Kafka brokers 中的 consumer 组提交的偏移量中开始读取分区
        // 如果找不到分区的偏移量，那么将会使用配置中的 auto.offset.reset 设置
        // kafka.setStartFromGroupOffsets();
        // ------------------------------------------------------------------------------------------------
        // 如果启用了 checkpoint，允许将 offset 提交回 Kafka broker，同时忽略 Properties 中关于 offset 提交的设置
        // 如果未启用 checkpoint，需要在 Properties 设置 "auto.commit.enable" (for 0.8) / "enable.auto.commit" (for 0.9+)
        kafka.setCommitOffsetsOnCheckpoints(true);
        // ------------------------------------------------------------------------------------------------
        // 为每个分区指定 consumer 应该开始消费的具体 offset
        // Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        // specificStartOffsets.put(new KafkaTopicPartition(kafkaTopic, 0), 23L);
        // specificStartOffsets.put(new KafkaTopicPartition(kafkaTopic, 1), 31L);
        // specificStartOffsets.put(new KafkaTopicPartition(kafkaTopic, 2), 43L);
        // kafka.setStartFromSpecificOffsets(specificStartOffsets);

        DataStream<Log> kafkaSource = env.addSource(kafka)
                .map(new MapFunction<String, Log>() {
                    private static final long serialVersionUID = -7472044796586964200L;

                    private final ObjectMapper mapper = new ObjectMapper();

                    @Override
                    public Log map(String value) throws Exception {
                        return mapper.readValue(value, Log.class);
                    }
                })
                // 自 Kafka 0.10+ 开始，Kafka 的消息可以携带时间戳，指示事件发生的时间或消息写入 Kafka broker 的时间
                // 如果 Flink 中的时间特性设置为 TimeCharacteristic.EventTime，则 FlinkKafkaConsumer010 将发出附加时间戳的记录
                // Kafka consumer 不会发出 watermark。为了发出 watermark，需要调用 assignTimestampsAndWatermarks 方法
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        // 如果保证 Kafka 每个分区事件时间戳是单调递增的，可以使用递增时间戳水印生成器，否则应该使用乱序时间戳水印生成器
                        // 为 Kafka Source 生成水印时，会在 Kafka Consumer 内部针对每个分区分别生成水印，
                        // 在后续处理中，每个分区的水印会像 stream shuffle 时水印合并那样进行合并
                        .<Log>forMonotonousTimestamps()
                        // 乱序时间戳水印生成器，参数 maxOutOfOrderness 表示当带有时间戳 T 的事件到达时，
                        // 不会有比 T - maxOutOfOrderness 更早的事件还没有到达
                        // .<Log>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Log>() {
                            private static final long serialVersionUID = -3155485859894626977L;

                            @Override
                            public long extractTimestamp(Log element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        })
                );

        kafkaSource.print();


        // ==============================================================
        //                  Flink Kafka Producer
        // ==============================================================
        /*
         * 启用 Flink 的 checkpoint 后，FlinkKafkaProducer010 可以提供至少一次的语义，
         * FlinkKafkaProducer011（适用于 Kafka >= 1.0.0 版本的 FlinkKafkaProducer）可以提供精准一次的语义保证
         *
         * 除了启用 Flink 的 checkpoint，还可以通过 semantic 参数传递给 FlinkKafkaProducer011(FlinkKafkaProducer)来选择三种不同的操作模式：
         * Semantic.NONE：Flink 不会有任何语义的保证，产生的记录可能会丢失或重复
         * Semantic.AT_LEAST_ONCE（默认）：类似 FlinkKafkaProducer010 中的 setFlushOnCheckpoint(true)，这可以保证不会丢失任何记录（虽然记录可能会重复）
         * Semantic.EXACTLY_ONCE：使用 Kafka 事务提供精准一次的语义
         *          无论何时，在使用事务写入 Kafka 时，都要记得为所有消费 Kafka 消息的应用程序设置所需的 isolation.level
         *          (read_committed 或 read_uncommitted - 后者是默认值)
         *
         * 注意：1.Semantic.EXACTLY_ONCE 模式依赖于事务提交的能力。事务提交发生于触发 checkpoint 之前，以及从 checkpoint 恢复之后。
         *        如果从 Flink 应用程序崩溃到完全重启的时间超过了 Kafka 的事务超时时间，那么将会有数据丢失（Kafka 会自动丢弃超出超时时间的事务）。
         *        默认情况下，Kafka broker 将 transaction.max.timeout.ms 设置为 15 分钟。此属性不允许为大于其值的 producer 设置事务超时时间。
         *        默认情况下，FlinkKafkaProducer011 将 producer config 中的 transaction.timeout.ms 属性设置为 1 小时，
         *        因此在使用 Semantic.EXACTLY_ONCE 模式之前应该增加 server 端 transaction.max.timeout.ms 的值。
         *      2.Semantic.EXACTLY_ONCE 模式为每个 FlinkKafkaProducer011 实例使用固定大小的 KafkaProducer 池。
         *        每个 checkpoint 使用其中一个 producer。如果并发 checkpoint 的数量超过池的大小，FlinkKafkaProducer011 将抛出异常，
         *        并导致整个应用程序失败。请合理地配置最大池大小和最大并发 checkpoint 数量。
         */
        String producerTopic = params.get("producer-topic", "test2");
        Properties props = new Properties();
        props.put("bootstrap.servers", params.get("producer-brokers", "localhost:9092"));
        // 重要：使用 Semantic.EXACTLY_ONCE 模式 Kafka Server 应该增大 transaction.max.timeout.ms 的值，这里减少 producer 的值进行模拟
        props.put("transaction.timeout.ms", String.valueOf(5000));
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(producerTopic,
                new SimpleStringSchema(), props, new FlinkFixedPartitioner<String>(),
                // 设置语义为 EXACTLY_ONCE
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE, 10);
        // 0.10+ 版本的 Kafka 允许在将记录写入 Kafka 时附加记录的事件时间戳；
        // 此方法不适用于早期版本的 Kafka
        kafkaProducer.setWriteTimestampToKafka(true);

        kafkaSource.map(new MapFunction<Log, String>() {
            private static final long serialVersionUID = -6919980416290466683L;

            private final ObjectMapper mapper = new ObjectMapper();

            @Override
            public String map(Log value) throws Exception {
                return mapper.writeValueAsString(value);
            }
        })
                .addSink(kafkaProducer);


        env.execute("Flink Kafka Connector");
    }
}
