package com.github.streaming.connect;

import com.github.streaming.connect.pojo.Goods;
import com.github.streaming.connect.pojo.Order;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Objects;
import java.util.Properties;

/**
 * 微信链接：https://mp.weixin.qq.com/s?__biz=MzI0NTIxNzE1Ng==&mid=2651219015&idx=1&sn=8be56c0e832be296bc5e4401e8475f4d&chksm=f2a326acc5d4afba2f26de6a192dbadaf8800407a1df4634bdb904512b30f6222a3c834fd146&mpshare=1&scene=1&srcid=&sharer_sharetime=1586328467820&sharer_sha
 * Github 链接：https://github.com/1996fanrui/fanrui-learning/blob/master/module-flink/src/main/java/com/dream/flink/connect/BroadcastOrderJoinGoodsName.java
 *
 * 实时处理订单信息，关联维表进行拼接发往下游
 * 两个 topic：
 *      order_topic_name     存放的订单的交易信息
 *      goods_dim_topic_name 存放商品 id 与商品名称的映射关系
 */
public class BroadcastDimension {

    private static final String KAFKA_ORDER_TOPIC = "order_topic_name";
    private static final String KAFKA_GOODS_TOPIC = "goods_dim_topic_name";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String brokers = params.get("consumer-brokers", "localhost:9092");
        String groupId = params.get("consumer-group-id", "test");

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", brokers);
        kafkaProps.setProperty("group.id", groupId);
        FlinkKafkaConsumer<String> orderStream = new FlinkKafkaConsumer<String>(KAFKA_ORDER_TOPIC, new SimpleStringSchema(),
                kafkaProps);
        DataStream<Order> orderDataStream = env.addSource(orderStream)
                .uid(KAFKA_ORDER_TOPIC)
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
                });

        // 维表
        FlinkKafkaConsumer<String> goodsStream = new FlinkKafkaConsumer<String>(KAFKA_GOODS_TOPIC, new SimpleStringSchema(),
                kafkaProps);
        DataStream<Goods> goodsDim = (SingleOutputStreamOperator<Goods>) env.addSource(goodsStream)
                .uid(KAFKA_GOODS_TOPIC)
                .filter(Objects::nonNull)
                .map(new MapFunction<String, Goods>() {

                    private transient ObjectMapper jsonParser;

                    @Override
                    public Goods map(String value) throws Exception {
                        if (jsonParser == null) {
                            jsonParser = new ObjectMapper();
                        }

                        return jsonParser.readValue(value, Goods.class);
                    }
                });

        // 存储 维度信息的 MapState
        final MapStateDescriptor<Integer, String> GOODS_STATE = new MapStateDescriptor<>(
                "GOODS_STATE",
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        orderDataStream
                // 订单流 与 维度信息的广播流 进行 connect
                .connect(goodsDim.broadcast(GOODS_STATE))
                .process(new BroadcastProcessFunction<Order, Goods, Tuple2<Order, String>>() {

                    /** 处理 订单信息，将订单信息与对应的商品名称进行拼接，一起发送到下游。 */
                    @Override
                    public void processElement(Order value, ReadOnlyContext ctx, Collector<Tuple2<Order, String>> out) throws Exception {
                        ReadOnlyBroadcastState<Integer, String> broadcastState =
                                ctx.getBroadcastState(GOODS_STATE);
                        // 从状态中获取 商品名称，拼接后发送到下游
                        String goodsName = broadcastState.get(value.goodsId);
                        out.collect(Tuple2.of(value, goodsName));
                    }

                    /** // 更新商品的维表信息到状态中 */
                    @Override
                    public void processBroadcastElement(Goods value, Context ctx,
                                                        Collector<Tuple2<Order, String>> out) throws Exception {
                        BroadcastState<Integer, String> broadcastState = ctx.getBroadcastState(GOODS_STATE);
                        if (value.isRemove) {
                            // 商品下架了，应该要从状态中移除，否则状态将无限增大
                            broadcastState.remove(value.goodsId);
                        } else {
                            // 商品上架，应该添加到状态中，用于关联商品信息
                            broadcastState.put(value.goodsId, value.goodsName);
                        }
                    }
                }).print();

        env.execute("Flink Broadcast Dimension.");

    }
}
