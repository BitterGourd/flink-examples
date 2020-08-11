package com.github.streaming.async;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExecutorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 异步函数使用案例 {@link AsyncFunction}
 *
 * 异步函数是指通过事件循环异步执行的函数，它会通过一个隐式的 Promise 返回其结果
 * 如果在代码中使用了异步函数，就会发现它的语法和结构会更像是标准的同步函数
 */
public class AsyncIOExample {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncIOExample.class);

    private static final String EXACTLY_ONCE_MODE = "exactly_once";
    private static final String EVENT_TIME = "EventTime";
    private static final String INGESTION_TIME = "IngestionTime";
    private static final String ORDERED = "ordered";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        final String statePath;
        final String cpMode;
        final int maxCount;
        final long sleepFactor;
        final float failRatio;
        final String mode;
        final int taskNum;
        final String timeType;
        final long shutdownWaitTS;
        final long timeout;

        try {
            statePath = params.get("fsStatePath", null);
            cpMode = params.get("checkpointMode", "exactly_once");
            maxCount = params.getInt("maxCount", 100000);
            sleepFactor = params.getLong("sleepFactor", 100);
            failRatio = params.getFloat("failRatio", 0.001f);
            mode = params.get("waitMode", "ordered");
            taskNum = params.getInt("waitOperatorParallelism", 1);
            timeType = params.get("eventType", "EventTime");
            shutdownWaitTS = params.getLong("shutdownWaitTS", 20000);
            timeout = params.getLong("timeout", 10000L);
        } catch (Exception e) {
            printUsage();

            throw e;
        }

        StringBuilder configStringBuilder = new StringBuilder();

        final String lineSeparator = System.getProperty("line.separator");
        configStringBuilder
                .append("Job configuration").append(lineSeparator)
                .append("FS state path=").append(statePath).append(lineSeparator)
                .append("Checkpoint mode=").append(cpMode).append(lineSeparator)
                .append("Max count of input from source=").append(maxCount).append(lineSeparator)
                .append("Sleep factor=").append(sleepFactor).append(lineSeparator)
                .append("Fail ratio=").append(failRatio).append(lineSeparator)
                .append("Waiting mode=").append(mode).append(lineSeparator)
                .append("Parallelism for async wait operator=").append(taskNum).append(lineSeparator)
                .append("Event type=").append(timeType).append(lineSeparator)
                .append("Shutdown wait timestamp=").append(shutdownWaitTS);

        LOG.info(configStringBuilder.toString());

        if (statePath != null) {
            env.setStateBackend(new FsStateBackend(statePath));
        }

        if (EXACTLY_ONCE_MODE.equals(cpMode)) {
            env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        } else {
            env.enableCheckpointing(1000L, CheckpointingMode.AT_LEAST_ONCE);
        }

        // watermark
        if (EVENT_TIME.equals(timeType)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        } else if (INGESTION_TIME.equals(timeType)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        }

        DataStream<Integer> inputStream = env.addSource(new SimpleSource(maxCount));

        // 异步函数，通过等待模拟异步 IO
        AsyncFunction<Integer, String> function =
                new SampleAsyncFunction(sleepFactor, failRatio, shutdownWaitTS);

        DataStream<String> result;
        if (ORDERED.equals(mode)) {
            result = AsyncDataStream
                    // 输出与输入同序
                    .orderedWait(inputStream, function, timeout, TimeUnit.MILLISECONDS, 20)
                    .setParallelism(taskNum);
        } else {
            result = AsyncDataStream
                    // 输出与输入可能乱序
                    .unorderedWait(inputStream, function, timeout, TimeUnit.MILLISECONDS, 20)
                    .setParallelism(taskNum);
        }

        // 针对每个 key 求和
        result.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 3196832886478433687L;

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(value, 1));
            }
        })
                .keyBy(0)
                .sum(1)
                .print();

        env.execute("Flink Streaming Async IO");
    }

    /**
     * A checkpointed source.
     */
    private static class SimpleSource implements SourceFunction<Integer>, CheckpointedFunction {

        private static final long serialVersionUID = 6885168497480951862L;

        private volatile boolean isRunning = true;
        private int counter = 0;
        private int start = 0;

        private ListState<Integer> state;

        public SimpleSource(int maxNum) {
            this.counter = maxNum;
        }

        /** 在 checkpoint 时调用 */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.add(start);
        }

        /** 在 parallel function 初始化(第一次初始化或从前一次 checkpoint 恢复)时调用 */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Integer>("state", IntSerializer.INSTANCE));

            // 初次启动一定为空，否则进行状态恢复
            for (Integer i : state.get()) {
                start = i;
            }
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while ((start < counter || counter == -1) && isRunning) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(start++);

                    // 防止越界
                    if (start == Integer.MAX_VALUE) {
                        start = 0;
                    }
                }

                Thread.sleep(10L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * An example of {@link AsyncFunction} using a thread pool and executing working threads
     * to simulate multiple async operations.
     *
     * For the real use case in production environment, the thread pool may stay in the
     * async client.
     */
    @SuppressWarnings("serial")
    private static class SampleAsyncFunction extends RichAsyncFunction<Integer, String> {

        private transient ExecutorService executorService;

        /**
         * The result of multiplying sleepFactor with a random float is used to pause
         * the working thread in the thread pool, simulating a time consuming async operation.
         */
        private final long sleepFactor;

        /**
         * The ratio to generate an exception to simulate an async error. For example, the error
         * may be a TimeoutException while visiting HBase.
         */
        private final float failRatio;

        private final long shutdownWaitTS;

        SampleAsyncFunction(long sleepFactor, float failRatio, long shutdownWaitTS) {
            this.sleepFactor = sleepFactor;
            this.failRatio = failRatio;
            this.shutdownWaitTS = shutdownWaitTS;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            executorService = Executors.newFixedThreadPool(30);
        }

        @Override
        public void close() throws Exception {
            super.close();
            // 优雅关闭线程池
            ExecutorUtils.gracefulShutdown(shutdownWaitTS, TimeUnit.MILLISECONDS, executorService);
        }

        @Override
        public void asyncInvoke(Integer input, ResultFuture<String> resultFuture) throws Exception {
            executorService.submit(() -> {
                // 等待一段时间来模拟异步操作
                long sleep = (long) (ThreadLocalRandom.current().nextFloat() * sleepFactor);
                try {
                    Thread.sleep(sleep);

                    if (ThreadLocalRandom.current().nextFloat() < failRatio) {
                        // 异常结束
                        resultFuture.completeExceptionally(new Exception("wahahaha..."));
                    } else {
                        resultFuture.complete(Collections.singletonList("key-" + (input % 10)));
                    }
                } catch (InterruptedException e) {
                    resultFuture.complete(new ArrayList<>(0));
                }
            });
        }
    }

    public static void printUsage() {
        System.out.println("To customize example, use: AsyncIOExample [--fsStatePath <path to fs state>] " +
                "[--checkpointMode <exactly_once or at_least_once>] " +
                "[--maxCount <max number of input from source, -1 for infinite input>] " +
                "[--sleepFactor <interval to sleep for each stream element>] [--failRatio <possibility to throw " +
                "exception>] " +
                "[--waitMode <ordered or unordered>] [--waitOperatorParallelism <parallelism for async wait " +
                "operator>] " +
                "[--eventType <EventTime or IngestionTime>] [--shutdownWaitTS <milli sec to wait for thread pool>]" +
                "[--timeout <Timeout for the asynchronous operations>]");
    }
}
