package com.github.streaming.join;

import com.github.streaming.state.pojo.Action;
import com.github.streaming.state.pojo.Pattern;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Flink Broadcast State - 广播状态
 * 原文链接：https://cloud.tencent.com/developer/article/1507365
 *
 * actions 流是用户的行为记录 (用户登录、用户注销、添加到购物车或完成支付))
 * patterns 流是用于评估用户的行为模式，由两个连续行为组成：
 *      1.用户登录并立即退出而不浏览电子商务网站上的其他页面
 *      2.用户向购物车中添加一个物品，然后在没有完成购买的情况下注销
 *
 * 注意事项：
 *   1.没有跨 task 通讯：
 *      只有在 (Keyed)-BroadcastProcessFunction 中处理广播流元素的方法里可以更改 broadcast state 的内容
 *      同时，用户需要保证所有 task 对于 broadcast state 的处理方式是一致的，
 *      否则会造成不同 task 读取 broadcast state 时内容不一致的情况，最终导致结果不一致
 *   2.broadcast state 在不同的 task 的事件顺序可能是不同的：
 *      虽然广播流中元素的过程能够保证所有的下游 task 全部能够收到，但在不同 task 中元素的到达顺序可能不同
 *      所以 broadcast state 的更新不能依赖于流中元素到达的顺序
 *   3.所有的 task 均会对 broadcast state 进行 checkpoint：
 *      虽然所有 task 中的 broadcast state 是一致的，但当 checkpoint 来临时所有 task 均会对 broadcast state 做 checkpoint
 *      这个设计是为了防止在作业恢复后读文件造成的文件热点。当然这种方式会造成 checkpoint 一定程度的写放大，放大倍数为 p（=并行度）
 *      Flink 会保证在恢复状态/改变并发的时候数据没有重复且没有缺失
 *      在作业恢复时，如果与之前具有相同或更小的并发度，所有的 task 读取之前已经 checkpoint 过的 state
 *      在增大并发的情况下，task 会读取本身的 state，多出来的并发（p_new - p_old）会使用轮询调度算法读取之前 task 的 state
 *  4.不使用 RocksDB state backend：
 *      broadcast state 在运行时保存在内存中，需要保证内存充足。这一特性同样适用于所有其他 Operator State
 */
public class BroadcastStreamConnect {

    private static final String USER_LOGIN = "User login";
    private static final String ADD_TO_CART = "Add to cart";
    private static final String PAYMENT_COMPLETE = "Payment complete";
    private static final String USER_LOGOUT = "User logout";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Action> actions = env.fromCollection(Arrays.asList(
                new Action(1001, USER_LOGOUT),
                new Action(1002, ADD_TO_CART),
                new Action(1003, PAYMENT_COMPLETE),
                new Action(1002, USER_LOGOUT)
        ));
        DataStream<Pattern> patterns = env.fromCollection(Arrays.asList(
                new Pattern(ADD_TO_CART, USER_LOGOUT),
                new Pattern(USER_LOGIN, USER_LOGOUT)
        ));

        KeyedStream<Action, Long> actionsByUser = actions.keyBy(new KeySelector<Action, Long>() {
            private static final long serialVersionUID = 3132575999519247720L;

            @Override
            public Long getKey(Action value) throws Exception {
                return value.getUserId();
            }
        });

        MapStateDescriptor<Void, Pattern> broadcastStateDescriptor =
                new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));

        BroadcastStream<Pattern> broadcastPatterns = patterns.broadcast(broadcastStateDescriptor);

        actionsByUser
                // connect() 方法需要由非广播流来进行调用，BroadcastStreamConnect 作为参数传入
                .connect(broadcastPatterns)
                .process(new PatternEvaluator())
                .print();

        env.execute("Flink Streaming Broadcast State");
    }

    /**
     * KeyedBroadcastProcessFunction 中的类型参数表示：
     *      1. key stream 中的 key 类型
     *      2. 非广播流中的元素类型
     *      3. 广播流中的元素类型
     *      4. 结果的类型
     */
    @SuppressWarnings("serial")
    private static class PatternEvaluator
            extends KeyedBroadcastProcessFunction<Long, Action, Pattern, Tuple2<Long, Pattern>> {

        // handle for keyed state (per user)
        ValueState<String> prevActionState;
        // broadcast state descriptor
        MapStateDescriptor<Void, Pattern> patterDesc;

        @Override
        public void open(Configuration parameters) throws Exception {
            // initialize keyed state
            prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastAction",
                    Types.STRING));
            patterDesc = new MapStateDescriptor<Void, Pattern>("patterns", Types.VOID, Types.POJO(Pattern.class));
        }

        /**
         * 处理非广播流
         * Called for each user action.
         * Evaluates the current pattern against the previous and
         * current action of the user.
         */
        @Override
        public void processElement(Action value, ReadOnlyContext ctx, Collector<Tuple2<Long, Pattern>> out) throws Exception {
            // get current pattern from broadcast state
            Pattern pattern = ctx.getBroadcastState(this.patterDesc)
                    // access MapState with null as VOID default value
                    .get(null);

            // get previous action of current user from keyed state
            String prevAction = prevActionState.value();
            if (pattern != null && prevAction != null) {
                // user had an action before, check if pattern matches
                if (pattern.getFirstAction().equals(prevAction) && pattern.getSecondAction().equals(value.getAction())) {
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }

            // update keyed state and remember action for next pattern evaluation
            prevActionState.update(value.getAction());
        }

        /**
         * 处理广播流
         * Called for each new pattern.
         * Overwrites the current pattern with the new pattern.
         */
        @Override
        public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<Long, Pattern>> out) throws Exception {
            // store the new pattern by updating the broadcast state
            org.apache.flink.api.common.state.BroadcastState<Void, Pattern> broadcastState =
                    ctx.getBroadcastState(patterDesc);

            // storing in MapState with null as VOID default value
            broadcastState.put(null, value);
        }
    }
}
