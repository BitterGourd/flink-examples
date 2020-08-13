package com.github.streaming.state;

import com.github.streaming.state.dfa.State;
import com.github.streaming.state.event.Alert;
import com.github.streaming.state.event.Event;
import com.github.streaming.state.generator.EventsGeneratorSource;
import com.github.streaming.state.kafka.EventDeSerializer;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 状态机案例 (Flink 状态管理及容错机制)
 *
 * 首先 events 是一个 DataStream，通过 env.addSource 加载数据进来
 * 接下来有一个 DataStream 叫 alerts，先 keyby 一个 sourceAddress，然后在 flatMap 一个 StateMachineMapper
 * StateMachineMapper 就是一个状态机
 *
 * 状态机指有不同的状态与状态间有不同的转换关系的结合，以买东西的过程简单举例
 * 首先下订单，订单生成后状态为待付款
 * 当再来一个事件状态付款成功，则事件的状态将会从待付款变为已付款，待发货
 * 已付款，待发货的状态再来一个事件发货，订单状态将会变为配送中
 * 配送中的状态再来一个事件签收，则该订单的状态就变为已签收
 * 在整个过程中，随时都可以来一个事件，取消订单
 * 无论哪个状态，一旦触发了取消订单事件最终就会将状态转移到已取消，至此状态就结束了
 */
public class StateMachineExample {

    public static void main(String[] args) throws Exception {

        // ---- print some usage help ----

        System.out.println("Usage with built-in data generator: StateMachineExample [--error-rate " +
                "<probability-of-invalid-transition>] [--sleep <sleep-per-record-in-ms>]");
        System.out.println("Usage with Kafka: StateMachineExample --kafka-topic <topic> [--brokers <brokers>]");
        System.out.println("Options for both the above setups: ");
        System.out.println("\t[--backend <file|rocks>]");
        System.out.println("\t[--checkpoint-dir <filepath>]");
        System.out.println("\t[--async-checkpoints <true|false>]");
        System.out.println("\t[--incremental-checkpoints <true|false>]");
        System.out.println("\t[--output <filepath> OR null for stdout]");
        System.out.println();

        // ---- determine whether to use the built-in source, or read from Kafka ----

        final SourceFunction<Event> source;
        final ParameterTool params = ParameterTool.fromArgs(args);

        if (params.has("kafka-topic")) {
            String kafkaTopic = params.get("kafka-topic");
            String brokers = params.get("brokers", "localhost:9092");

            System.out.format("Reading from kafka topic %s @ %s%n%n", kafkaTopic, brokers);

            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("bootstrap.servers", brokers);

            FlinkKafkaConsumer<Event> kafka = new FlinkKafkaConsumer<Event>(kafkaTopic, new EventDeSerializer(),
                    kafkaProps);
            // 指定 Kafka 消费 offset 策略，如果已保存 checkpoint 或 savepoint，则此项配置不生效，以状态存储为准
            kafka.setStartFromLatest();
            // 是否自动提交 Offset，仅在开启 checkpoint 时有效
            // 否则应该使用 "auto.commit.enable" (for 0.8) / "enable.auto.commit" (for 0.9+)
            kafka.setCommitOffsetsOnCheckpoints(false);
            source = kafka;
        } else {
            double errorRate = params.getDouble("error-rate", 0.01);
            int sleep = params.getInt("sleep", 100);

            System.out.printf("Using standalone source with error rate %f and sleep delay %s millis%n%n", errorRate, sleep);

            source = new EventsGeneratorSource(errorRate, sleep);
        }

        // ---- main program ----

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);
        env.getConfig().setGlobalJobParameters(params);

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

        DataStream<Event> events = env.addSource(source);
        DataStream<Alert> alerts = events
                .keyBy(Event::sourceAddress)
                // StateMachineMapper - 状态机
                .flatMap(new StateMachineMapper());

        final String outputFile = params.get("output");
        if (outputFile == null) {
            alerts.print();
        } else {
            // 指定的是文件名，会写成 1 个文件
            // alerts.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            // 指定的是 1 个目录，会在此目录下创建子目录，然后才是写的文件
            alerts.addSink(StreamingFileSink.forRowFormat(new Path(outputFile),
                    new SimpleStringEncoder<Alert>()).build())
                    .setParallelism(1);
        }

        env.execute("Flink Streaming State Machine Job");
    }

    /**
     * The function that maintains the per-IP-address state machines and verifies that the
     * events are consistent with the current state of the state machine. If the event is not
     * consistent with the current state, the function produces an alert.
     */
    @SuppressWarnings("serial")
    static class StateMachineMapper extends RichFlatMapFunction<Event, Alert>{

        /** The state for the current key. */
        private ValueState<State> currentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            currentState = getRuntimeContext().getState(new ValueStateDescriptor<State>("state", State.class));
        }

        @Override
        public void flatMap(Event value, Collector<Alert> out) throws Exception {
            // 获取当前 key 的当前状态，如果不存在状态，那必然是 Initial 状态
            State state = currentState.value();
            if (state == null) {
                state = State.Initial;
            }

            // 向状态机询问下一个状态
            State nextState = state.transition(value.type());

            if (nextState == State.InvalidTransition) {
                // 当前事件会导致进入 InvalidTransition (非法状态)
                // 告警!
                out.collect(new Alert(value.sourceAddress(), state, value.type()));
            } else if (nextState.isTerminal()) {
                // 当前状态已经是最终状态，清楚当前 key 的状态
                currentState.clear();
            } else {
                // 更新当前状态
                currentState.update(nextState);
            }
        }
    }
}
