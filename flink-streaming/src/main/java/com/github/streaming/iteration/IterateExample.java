package com.github.streaming.iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Flink 流处理之迭代案例：
 *      1.Source 生成 Pair 随机数对 (各数在 1 到 BOUND 之间)
 *      2.进行斐波那契迭代
 *      3.当迭代到大于 BOUND 时停止迭代，进行输出，输出格式 ((初始随机数 x, 初始随机数 y), 迭代次数 count)
 *
 * Flink 实现迭代的思路：
 *      实现一个 step 函数，然后将其嵌入到迭代算子中去
 *      有两种迭代操作算子 Iterate和 Delta Iterate。两个操作算子都是在未收到终止迭代信号之前一直调用 step 函数
 *      每次迭代，step 函数会消费全量数据(本次输入和上次迭代的结果)，然后计算得到下轮迭代的输出(例如，map、reduce、join等)
 *
 * 核心概念：
 *      1.迭代输入(Iteration Input)：第一次迭代的初始输入，可能来源于数据源或者先前的操作算子
 *      2.Step 函数：每次迭代都会执行 step 函数。其是由 map，reduce，join 等算子组成的数据流，根据业务定制的
 *      3.下次迭代的部分结果(Next Partial Solution)：每次迭代，step 函数的输出结果会有部分返回参与继续迭代
 *      4.最大迭代次数：如果没有其他终止条件，就会在聚合次数达到该值的情况下终止
 *      5.自定义聚合器收敛：迭代允许指定自定义聚合器和收敛标准，如 sum 会聚合要发出的记录数（聚合器），如果此数字为零则终止（收敛标准）
 */
public class IterateExample {

    private static final int BOUND = 100;

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setBufferTimeout(1);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Tuple2<Integer, Integer>> inputStream;
        if (params.has("input")) {
            inputStream = env.readTextFile(params.get("input")).map(new FibonacciInputMap());
        } else {
            System.out.println("Executing Iterate example with default input data set.");
            System.out.println("Use --input to specify file input.");
            inputStream = env.addSource(new RandomFibonacciSource());
        }

        // create an iterative data stream from the input with 5 second timeout
        IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> it = inputStream
                /*
                 * Map the inputs so that the next Fibonacci numbers can be calculated while preserving the original
                 * input tuple. A counter is attached to the tuple and incremented in every iteration step.
                 */
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple5<Integer, Integer, Integer,
                        Integer, Integer>>() {
                    private static final long serialVersionUID = 5031706296822622108L;

                    @Override
                    public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                        return new Tuple5<>(value.f0, value.f1, value.f0, value.f1, 0);
                    }
                })
                .iterate(5000L);

        // apply the step function to get the next Fibonacci number
        // increment the counter and split the output with the output selector
        SplitStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> step = it
                .map(new Step())
                .split(new MySelector());

        // close the iteration by selecting the tuples that were directed to the
        // 'iterate' channel in the output selector
        it.closeWith(step.select("iterate"));

        // to produce the final output select the tuples directed to the
        // 'output' channel then get the input pairs that have the greatest iteration counter
        // on a 1 second sliding window
        DataStream<Tuple2<Tuple2<Integer, Integer>, Integer>> numbers = step
                .select("output")
                /* Giving back the input pair and the counter. */
                .map(new MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple2<Tuple2<Integer,
                        Integer>, Integer>>() {
                    private static final long serialVersionUID = -438020003760137256L;

                    @Override
                    public Tuple2<Tuple2<Integer, Integer>, Integer> map(Tuple5<Integer, Integer, Integer, Integer,
                            Integer> value) throws Exception {
                        return new Tuple2<>(new Tuple2<>(value.f0, value.f1), value.f4);
                    }
                });

        if (params.has("output")) {
            numbers.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            numbers.print();
        }

        env.execute("Flink Streaming Iteration");
    }

    /**
     * Generate random integer pairs from the range from 0 to BOUND/2.
     */
    private static class FibonacciInputMap implements MapFunction<String, Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 341020049098951273L;

        @Override
        public Tuple2<Integer, Integer> map(String value) throws Exception {
            String record = value.substring(1, value.length() - 1);
            String[] split = record.split(",");
            return new Tuple2<>(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
        }
    }

    /**
     * Generate BOUND number of random integer pairs from the range from 1 to BOUND/2.
     */
    private static class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 3956058209296927542L;

        private Random rnd = new Random();

        private volatile boolean isRunning = true;
        private int counter = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            while (isRunning && counter < BOUND) {
                int first = rnd.nextInt(BOUND / 2 - 1) + 1;
                int second = rnd.nextInt(BOUND / 2 - 1) + 1;

                ctx.collect(new Tuple2<>(first, second));
                counter++;
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * Iteration step function that calculates the next Fibonacci number.
     */
    public static class Step implements
            MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer,
                    Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple5<Integer, Integer, Integer, Integer,
                Integer> value) throws Exception {
            return new Tuple5<>(value.f0, value.f1, value.f3, value.f2 + value.f3, ++value.f4);
        }
    }

    /**
     * OutputSelector testing which tuple needs to be iterated again.
     */
    private static class MySelector implements OutputSelector<Tuple5<Integer, Integer, Integer, Integer, Integer>> {

        private static final long serialVersionUID = 1169136773586000785L;

        @Override
        public Iterable<String> select(Tuple5<Integer, Integer, Integer, Integer, Integer> value) {
            List<String> output = new ArrayList<>();
            if (value.f2 < BOUND && value.f3 < BOUND) {
                output.add("iterate");
            } else {
                output.add("output");
            }

            return output;
        }
    }
}
