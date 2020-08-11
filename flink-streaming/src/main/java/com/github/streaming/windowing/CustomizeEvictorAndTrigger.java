package com.github.streaming.windowing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 自定义 evictor 和 trigger
 */
public class CustomizeEvictorAndTrigger {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Tuple4<Integer, Integer, Double, Long>> carData;
        if (params.has("input")) {
            carData = env.readTextFile(params.get("input")).map(new ParseCarData());
        } else {
            System.out.println("Executing TopSpeedWindowing example with default input data set.");
            System.out.println("Use --input to specify file input.");
            carData = env.addSource(CarSource.create(2));
        }

        int evictionSec = 10;
        double triggerMeters = 50;
        DataStream<Tuple4<Integer, Integer, Double, Long>> topSpeeds = carData
                // 生成时间戳并创建 watermark
                .assignTimestampsAndWatermarks(new CarTimestamp())
                .keyBy(0)
                // window 参数需要一个 WindowAssigner(窗口分配器)，将数据流中的元素分配到对应的窗口
                // GlobalWindows 用于使用 Trigger(触发器) 和 Evictor(清除器) 来定义更灵活的窗口
                .window(GlobalWindows.create())
                /*
                 * Evictor 是在 WindowAssigner 和 Trigger 的基础上的一个可选选项，用来清除一些数据
                 * 可以在 Window Function 之前(evictBefore)，也可以在之后(evictAfter)
                 *
                 * evictor() 方法是可选的，如果不选择，则默认没有
                 *
                 * CountEvictor 保留指定数量的元素，多余的元素按照从前到后的顺序先后清理
                 * DeltaEvictor 通过执行用户给定的 DeltaFunction 以及预设的 threshold，判断是否删除一个元素
                 * TimeEvictor 设定一个 windowSize，删除所有不在 maxTs - windowSize 的元素，maxTs 是窗口内时间戳的最大值
                 */
                .evictor(TimeEvictor.of(Time.of(evictionSec, TimeUnit.SECONDS)))
                // Trigger 用来判断一个窗口是否需要被触发，每个窗口都自带一个默认的 Trigger
                // 决定了何时启动 Window Function 来处理窗口中的数据以及何时将窗口内的数据清理
                .trigger(DeltaTrigger.of(triggerMeters,
                        new DeltaFunction<Tuple4<Integer, Integer, Double, Long>>() {
                            private static final long serialVersionUID = -5412569253563063695L;

                            @Override
                            public double getDelta(Tuple4<Integer, Integer, Double, Long> oldDataPoint,
                                                   Tuple4<Integer, Integer, Double, Long> newDataPoint) {
                                return newDataPoint.f2 - oldDataPoint.f2;
                            }
                        }, carData.getType().createSerializer(env.getConfig())))
                .maxBy(1);

        topSpeeds.print();
        env.execute("Flink Streaming Window Customize Evictor And Trigger");
    }

    private static class ParseCarData extends RichMapFunction<String, Tuple4<Integer, Integer, Double, Long>> {

        private static final long serialVersionUID = -2068764182366692198L;

        @Override
        public Tuple4<Integer, Integer, Double, Long> map(String value) throws Exception {
            String rowData = value.substring(1, value.length() - 1);
            String[] data = rowData.split(",");
            return new Tuple4<>(Integer.parseInt(data[0]), Integer.parseInt(data[1]), Double.parseDouble(data[2]),
                    Long.parseLong(data[3]));
        }
    }

    private static class CarSource implements SourceFunction<Tuple4<Integer, Integer, Double, Long>> {

        private static final long serialVersionUID = -2132972624048575923L;
        private Integer[] speeds;
        private Double[] distances;

        private Random random = new Random();

        private volatile boolean isRunning = true;

        private CarSource(int numOfCars) {
            speeds = new Integer[numOfCars];
            distances = new Double[numOfCars];
            // 填充数据
            Arrays.fill(speeds, 50);
            Arrays.fill(distances, 0d);
        }

        public static CarSource create(int cars) {
            return new CarSource(cars);
        }

        @Override
        public void run(SourceContext<Tuple4<Integer, Integer, Double, Long>> ctx) throws Exception {
            while (isRunning) {
                Thread.sleep(100);
                for (int carId = 0; carId < speeds.length; carId++) {
                    if (random.nextBoolean()) {
                        speeds[carId] = Math.min(100, speeds[carId] + 5);
                    } else {
                        speeds[carId] = Math.max(0, speeds[carId] - 5);
                    }

                    distances[carId] += speeds[carId] / 3.6d;
                    Tuple4<Integer, Integer, Double, Long> record = new Tuple4<>(carId,
                            speeds[carId], distances[carId], System.currentTimeMillis());
                    ctx.collect(record);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class CarTimestamp extends AscendingTimestampExtractor<Tuple4<Integer, Integer, Double, Long>> {

        private static final long serialVersionUID = 8061015088577173343L;

        @Override
        public long extractAscendingTimestamp(Tuple4<Integer, Integer, Double, Long> element) {
            return element.f3;
        }
    }
}
