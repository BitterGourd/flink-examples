package com.github.streaming.cep;

import com.github.streaming.cep.pojo.Event;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 在 CEP 中，事件的处理顺序很重要。在使用事件时间时，为了保证事件按照正确的顺序被处理，
 * 一个事件到来后会先被放到一个缓冲区中，在缓冲区里事件都按照时间戳从小到大排序，当水位线到达后，
 * 缓冲区中所有小于水位线的事件被处理。这意味着水位线之间的数据都按照时间戳被顺序处理
 *
 * 为了保证跨水位线的事件按照事件时间处理，Flink CEP 库假定水位线一定是正确的，并且把时间戳小于最新水位线的事件看作是晚到的
 * 晚到的事件不会被处理，可以指定一个侧输出标志来收集比最新水位线晚到的事件
 */
@SuppressWarnings("serial")
public class CEPTimeout {

    private static final OutputTag<String> outputTag = new OutputTag<String>("late-data"){};

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStream<Event> input = env.fromElements(
                new Event(1, "foo", 1.0),
                new Event(2, "error", 2.0),
                new Event(3, "critical", 3.0),
                new Event(4, "bar", 4.0),
                new Event(5, "33", 5.0),
                new Event(6, "error", 6.0),
                new Event(7, "bar", 3.0),
                new Event(42, "55", 42.0),
                new Event(8, "error", 1.0)
        );

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
                .next("end").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().equals("error");
                    }
                }).within(Time.seconds(2));

        PatternStream<Event> patternStream = CEP.pattern(input, pattern);

        SingleOutputStreamOperator<String> result = patternStream.process(new MyPatternProcessFunction());
        DataStream<String> lateEvent = result.getSideOutput(outputTag);

        result.print("result");
        lateEvent.print("late");

        env.execute("Flin CEP Timeout");
    }

    private static class MyPatternProcessFunction extends PatternProcessFunction<Event, String>
            implements TimedOutPartialMatchHandler<Event> {

        /**
         * Context 继承自 {@link org.apache.flink.cep.time.TimeContext}
         * TimeContext 用在 {@link PatternProcessFunction} 和 {@link IterativeCondition} 中
         * 例如 {@link SimpleCondition} 就继承自 {@link IterativeCondition}
         *
         * TimeContext 拥有 2 个方法：
         *  1.timestamp() : 当前正处理的事件的时间戳，如果是 ProcessingTime，这个值会被设置为事件进入 CEP 算子的时间
         *  2.currentProcessingTime() : 当前的处理时间
         */
        @Override
        public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) throws Exception {
            String result = match.get("start").get(0).getId() + ","
                    + match.get("end").get(0).getId();

            out.collect(result);

            // System.out.println("processMatch timestamp:" + ctx.timestamp());
            // System.out.println("processMatch currentProcessingTime:" + ctx.currentProcessingTime());
        }

        @Override
        public void processTimedOutMatch(Map<String, List<Event>> match, Context ctx) throws Exception {
            Event startEvent = match.get("start").get(0);
            ctx.output(outputTag, startEvent.toString());

            // System.out.println("processTimedOutMatch timestamp:" + ctx.timestamp());
            // System.out.println("processTimedOutMatch currentProcessingTime:" + ctx.currentProcessingTime());
        }
    }
}
