package com.github.streaming.topn;

import com.github.streaming.topn.pojo.ItemViewCount;
import com.github.streaming.topn.pojo.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 原文链接 ： https://ververica.cn/developers/computing-real-time-hot-goods/
 * Github 项目地址： https://github.com/wuchong/my-flink-project/blob/master/src/main/java/myflink/HotItems.java
 *
 * 每隔 5分钟输出最近一小时内点击量最多的前 N 个商品
 * 数据集来源：阿里云天池公开数据集-淘宝用户行为数据集
 */
public class HotItems {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        PojoTypeInfo<UserBehavior> pojoType =
                (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = {"userId", "itemId", "categoryId", "behavior", "timestamp"};
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);

        env.createInput(csvInput, pojoType)
                // 生产环境一般用 WatermarkStrategy.forBoundedOutOfOrderness (1.11)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    private static final long serialVersionUID = -1124176673647143552L;

                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000;
                    }
                })
                // 过滤出点击数据
                .filter(new FilterFunction<UserBehavior>() {
                    private static final long serialVersionUID = 1194824835891589372L;

                    @Override
                    public boolean filter(UserBehavior value) throws Exception {
                        return "pv".equals(value.getBehavior());
                    }
                })
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                // 能使用 AggregateFunction 提前聚合掉数据，减少 state 的存储压力
                // 较之 .apply(WindowFunction wf) 会将窗口中的数据都存储下来，最后一起计算要高效地多
                .aggregate(new CountAgg(), new WindowResultFunction())
                .keyBy("windowEnd")
                .process(new TopNHotItems(3))
                .print();

        env.execute("Flink Streaming TopN Items");
    }

    /** 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串 */
    @SuppressWarnings("serial")
    private static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private final int topSize;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
        private ListState<ItemViewCount> itemState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<ItemViewCount> itemStateDesc = new ListStateDescriptor<>("item-state",
                    ItemViewCount.class);
            itemState = getRuntimeContext().getListState(itemStateDesc);
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 每条数据都保存到状态中
            itemState.add(value);
            // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于 windowEnd 窗口的所有商品数据 (因为数据无乱序)
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 获取收到的所有商品点击量
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }

            itemState.clear();

            // 按照点击量从大到小排序
            allItems.sort(Comparator.comparingLong(ItemViewCount::getViewCount).reversed());

            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
            for (int i = 0; i < allItems.size() && i < topSize; i++) {
                ItemViewCount currentItem = allItems.get(i);
                // No1:  商品ID=12224  浏览量=2413
                result.append("No").append(i + 1).append(":")
                        .append("  商品ID=").append(currentItem.getItemId())
                        .append("  浏览量=").append(currentItem.getViewCount())
                        .append("\n");
            }
            result.append("====================================\n\n");

            // 控制输出频率，模拟实时滚动结果
            Thread.sleep(1000);

            out.collect(result.toString());
        }
    }

    private static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        private static final long serialVersionUID = 4405587987360035078L;

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = ((Tuple1<Long>) tuple).f0;
            Long count = input.iterator().next();
            out.collect(ItemViewCount.of(itemId, window.getEnd(), count));
        }
    }

    /** COUNT 统计的聚合函数实现，每出现一条记录加一 */
    private static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        private static final long serialVersionUID = 8779979405807984889L;

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
