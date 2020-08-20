package com.github.streaming.cep;

import com.github.streaming.cep.pojo.Event;
import com.github.streaming.cep.pojo.SubEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Flink CEP 示例
 * 原文链接：https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/libs/cep.html
 * 迟到事件(指定 within 时)可以输出到侧输出流
 *
 * 注意：DataStream 中的事件，如果想在上面进行模式匹配的话，必须实现合适的 equals()和 hashCode()方法
 *      因为 FlinkCEP 使用它们来比较和匹配事件
 */
@SuppressWarnings("serial")
public class FlinkCEP {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Event> input = env.fromElements(
                new Event(1, "barfoo", 1.0),
                new Event(2, "start", 2.0),
                new Event(3, "foobar", 3.0),
                new SubEvent(4, "foo", 4.0, 1.0),
                new Event(5, "middle", 5.0),
                new SubEvent(6, "middle", 6.0, 2.0),
                new SubEvent(7, "bar", 3.0, 3.0),
                new Event(42, "42", 42.0),
                new Event(8, "end", 1.0)
        );

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().equals("start");
                    }
                })
                /*
                 * next()，指定严格连续
                 * followedBy()，指定松散连续
                 * followedByAny()，指定不确定的松散连续
                 *
                 * 举例来说，模式"a b"，给定事件序列"a"，"c"，"b1"，"b2"，会产生如下的结果：
                 *  "a"和"b"之间严格连续： {} （没有匹配），"a"之后的"c"导致"a"被丢弃
                 *  "a"和"b"之间松散连续： {a b1}，松散连续会"跳过不匹配的事件直到匹配上的事件"
                 *  "a"和"b"之间不确定的松散连续： {a b1}, {a b2}，这是最常见的情况
                 */
                .followedByAny("middle")
                .subtype(SubEvent.class)
                .where(new SimpleCondition<SubEvent>() {
                    @Override
                    public boolean filter(SubEvent value) throws Exception {
                        return value.getName().equals("middle");
                    }
                })
                .followedByAny("end")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getName().equals("end");
                    }
                });
                // .within(Time.seconds(10)) 表示此模式应该在10秒内发生
                // 一个模式序列只能有一个时间限制。如果限制了多个时间在不同的单个模式上，会使用最小的那个时间限制

        DataStream<String> result = CEP.pattern(input, pattern)
                // FLink 1.8 之后引入了 process(PatternProcessFunction)
                // flatSelect 和 select 都是旧版 API，但在内部会被转换为 PatternProcessFunction
                .flatSelect(new PatternFlatSelectFunction<Event, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void flatSelect(Map<String, List<Event>> pattern, Collector<String> out) throws Exception {

                        String builder = pattern.get("start").get(0).getId() + "," +
                                pattern.get("middle").get(0).getId() + "," +
                                pattern.get("end").get(0).getId();
                        out.collect(builder);
                    }
                }, Types.STRING);

        result.print();

        env.execute("Flink CEP");
    }
}
