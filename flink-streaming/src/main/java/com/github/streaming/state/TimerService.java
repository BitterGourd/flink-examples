package com.github.streaming.state;

import com.github.streaming.state.pojo.Alert;
import com.github.streaming.state.pojo.Transaction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * 基于 DataStream API 实现欺诈检测
 * 原文链接：https://ci.apache.org/projects/flink/flink-docs-master/zh/try-flink/datastream_api.html
 *
 * 对于一个账户，如果出现小于 $1 的交易后紧跟着一个大于 $500 的交易，并且 2 个交易间隔一分钟内，就认为是欺诈交易
 */
public class TimerService {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        if (!params.has("input-file")) {
            System.out.println("TimerService Usage: TimerService --input-file <file-path>");
            throw new IllegalStateException("没有输入文件，请指定参数 --input-file.");
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> transactions = env
                .readTextFile(params.get("input-file"))
                .map(new MapFunction<String, Transaction>() {
                    private static final long serialVersionUID = -5156021882266656155L;

                    @Override
                    public Transaction map(String value) throws Exception {
                        String[] split = value.split(",");
                        return Transaction.of(Long.parseLong(split[0]), Double.parseDouble(split[1]));
                    }
                })
                .name("transactions");

        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        alerts
                .print()
                .name("send-alerts");

        env.execute("Fraud Detection");
    }

    /**
     * 使用一个 boolean 型的标记状态来表示是否刚处理过一个小额交易
     *
     * 当标记状态被设置为 true 时，设置一个在当前时间一分钟后触发的定时器
     * 当定时器被触发时，重置标记状态
     * 当标记状态被重置时，删除定时器
     */
    private static class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

        private static final long serialVersionUID = 6060202317791168135L;

        private static final double SMALL_AMOUNT = 1.00;
        private static final double LARGE_AMOUNT = 500.00;
        private static final long ONE_MINUTE = 60 * 1000;

        private transient ValueState<Boolean> flagState;
        private transient ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<Boolean>(
                    "flag",
                    Types.BOOLEAN
            );
            flagState = getRuntimeContext().getState(flagDescriptor);

            ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<Long>(
                    "timer-state",
                    Types.LONG
            );
            timerState = getRuntimeContext().getState(timerDescriptor);
        }

        @Override
        public void processElement(Transaction value, Context ctx, Collector<Alert> out) throws Exception {
            // Get the current state for the current key
            Boolean lastTransactionWasSmall = flagState.value();

            // Check if the flag is set
            if (lastTransactionWasSmall != null) {
                if (value.getAmount() > LARGE_AMOUNT) {
                    //Output an alert downstream
                    Alert alert = new Alert();
                    alert.setAccountId(value.getAccountId());

                    out.collect(alert);
                }

                // Clean up our state
                cleanUp(ctx);
            }

            if (value.getAmount() < SMALL_AMOUNT) {
                // set the flag to true
                flagState.update(true);

                long timer = ctx.timerService().currentProcessingTime() + ONE_MINUTE;
                ctx.timerService().registerProcessingTimeTimer(timer);

                timerState.update(timer);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
            // remove flag after 1 minute
            timerState.clear();
            flagState.clear();
        }

        private void cleanUp(Context ctx) throws IOException {
            // delete timer
            Long timer = timerState.value();
            ctx.timerService().deleteProcessingTimeTimer(timer);

            // clean up all state
            timerState.clear();
            flagState.clear();
        }
    }
}
