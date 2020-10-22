package com.github.streaming.connect;

import com.github.streaming.connect.pojo.Order;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

/**
 * 双流 join
 *
 * 微信链接：https://mp.weixin.qq.com/s?__biz=MzUzMDYwOTAzOA==&mid=2247483756&idx=1&sn=e0033f4479be4cb72da4b6c5d3e7441b&chksm=fa4e678dcd39ee9b471f62c3db5c1651f5b7d5218b7784a9a0a45fb8a3c4f2276076a3a04f39&mpshare=1&scene=1&srcid=&sharer_sharetime=1586328514661&sharer_sha
 * Github 链接：https://github.com/1996fanrui/fanrui-learning/blob/master/module-flink/src/main/java/com/dream/flink/connect/OrderMatch.java
 *
 * 场景：
 *  一个订单分成了 大订单和小订单，需要在数据流中按照订单 Id 进行匹配，
 *  默认认为数据流的延迟最大为 60s。
 *  大订单和小订单匹配成功后向下游发送，若 60s 还未匹配成功，则测流输出
 *
 * 思路：
 *  提取时间戳，按照 orderId 进行 keyBy，然后两个流 connect，
 *  大订单和小订单的处理逻辑一样，两个流通过 State 进行关联。
 *  来了一个流，需要保存到自己的状态中，并注册一个 60s 之后的定时器。
 *  如果 60s 内来了第二个流，则将两个数据拼接发送到下游。
 *  如果 60s 内第二个流还没来，就会触发 onTimer，然后进行侧流输出。
 *
 *  提示：如果将此代码应用到生产环境，需要考虑个别分区(某一段时间)无数据流到达的情况，这样的话 onTimer 会很久不会执行
 */
public class DoubleStreamMatch {

    private static final OutputTag<Order> bigOrderTag = new OutputTag<Order>("bigOrder") {
    };
    private static final OutputTag<Order> smallOrderTag = new OutputTag<Order>("smallOrder") {
    };

    private static final String BIG_ORDER_TOPIC = "big_order_topic_name";
    private static final String SMALL_ORDER_TOPIC = "small_order_topic_name";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        String brokers = params.get("consumer-brokers", "localhost:9092");
        String groupId = params.get("consumer-group-id", "test");
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", brokers);
        kafkaProps.setProperty("group.id", groupId);

        // ================  大订单  ========================
        FlinkKafkaConsumer<String> bigOrderConsumer = new FlinkKafkaConsumer<String>(BIG_ORDER_TOPIC, new SimpleStringSchema(),
                kafkaProps);
        // bigOrderConsumer.setStartFromEarliest();
        KeyedStream<Order, String> bigOrderStream = env.addSource(bigOrderConsumer)
                .uid(BIG_ORDER_TOPIC)
                .filter(Objects::nonNull)
                .map(new MapFunction<String, Order>() {
                    private transient ObjectMapper jsonParser;

                    @Override
                    public Order map(String value) throws Exception {
                        if (jsonParser == null) {
                            jsonParser = new ObjectMapper();
                        }

                        return jsonParser.readValue(value, Order.class);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                            @Override
                            public long extractTimestamp(Order element, long recordTimestamp) {
                                return element.time;
                            }
                        }))
                .keyBy(order -> order.orderId);

        // ================  小订单  ========================
        FlinkKafkaConsumer<String> smallOrderConsumer = new FlinkKafkaConsumer<String>(SMALL_ORDER_TOPIC, new SimpleStringSchema(),
                kafkaProps);
        // smallOrderConsumer.setStartFromEarliest();
        KeyedStream<Order, String> smallOrderStream = env.addSource(smallOrderConsumer)
                .uid(SMALL_ORDER_TOPIC)
                .filter(Objects::nonNull)
                .map(new MapFunction<String, Order>() {
                    private transient ObjectMapper jsonParser;

                    @Override
                    public Order map(String value) throws Exception {
                        if (jsonParser == null) {
                            jsonParser = new ObjectMapper();
                        }

                        return jsonParser.readValue(value, Order.class);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                            @Override
                            public long extractTimestamp(Order element, long recordTimestamp) {
                                return element.time;
                            }
                        }))
                .keyBy(order -> order.orderId);

        // 使用 connect 连接大小订单的流，然后使用 CoProcessFunction 进行数据匹配
        SingleOutputStreamOperator<Tuple2<Order, Order>> resStream = bigOrderStream
                .connect(smallOrderStream)
                .process(new OrderMatcher());

        resStream.print("connected");

        // 只有大订单时，没有匹配到 小订单，属于异常数据，需要保存到外部系统，进行特殊处理
        resStream.getSideOutput(bigOrderTag).print();
        // 只有小订单时，没有匹配到 大订单，属于异常数据，需要保存到外部系统，进行特殊处理
        resStream.getSideOutput(smallOrderTag).print();

        env.execute("Flink Double Stream Match.");
    }

    @SuppressWarnings("serial")
    private static class OrderMatcher extends KeyedCoProcessFunction<String, Order, Order, Tuple2<Order, Order>> {

        // 大订单数据先来了，将大订单数据保存在 bigState 中。
        private transient ValueState<Order> bigState;
        // 小订单数据先来了，将小订单数据保存在 smallState 中。
        private transient ValueState<Order> smallState;
        // 当前注册的 定时器的 时间戳
        private transient ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            bigState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("bigState", Order.class));
            smallState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("smallState", Order.class));
            timerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("timerState", Types.LONG));
        }

        @Override
        public void processElement1(Order bigOrder, Context ctx, Collector<Tuple2<Order, Order>> out) throws Exception {

            Order smallOrder = smallState.value();
            if (smallOrder != null) {
                System.out.println("smallOrder 先到达 ...");
                // 小订单先来了，直接将大小订单拼接发送到下游
                out.collect(Tuple2.of(smallOrder, bigOrder));

                smallState.clear();

                ctx.timerService().deleteEventTimeTimer(timerState.value());
                timerState.clear();
            } else {
                // 小订单还没来，将大订单放到状态中，并注册 1 分钟之后触发的 timerState
                bigState.update(bigOrder);

                long time = bigOrder.time + 60000L;
                timerState.update(time);

                ctx.timerService().registerEventTimeTimer(time);
            }
        }

        @Override
        public void processElement2(Order smallOrder, Context ctx, Collector<Tuple2<Order, Order>> out) throws Exception {

            // 小订单的处理逻辑与大订单的处理逻辑完全类似
            Order bigOrder = bigState.value();
            if (bigOrder != null) {
                System.out.println("bigOrder 先到达 ...");
                out.collect(Tuple2.of(smallOrder, bigOrder));

                bigState.clear();

                ctx.timerService().deleteEventTimeTimer(timerState.value());
                timerState.clear();
            } else {
                smallState.update(smallOrder);

                long time = smallOrder.time + 60000L;
                timerState.update(time);

                ctx.timerService().registerEventTimeTimer(time);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Order, Order>> out) throws Exception {
            System.out.println("定时器触发时间：" + timestamp);

            // 定时器触发了，即 1 分钟内没有接收到两个流。
            // 大订单不为空，则将大订单信息侧流输出
            if (bigState.value() != null) {
                ctx.output(bigOrderTag, bigState.value());
            }

            // 小订单不为空，则将小订单信息侧流输出
            if (smallState.value() != null) {
                ctx.output(smallOrderTag, smallState.value());
            }

            bigState.clear();
            smallState.clear();
            timerState.clear();
        }
    }
}
