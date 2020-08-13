## Flink Window

> 本文摘自以下文章
>
> InfoQ [Apache Flink 零基础入门（五）：流处理核心组件 Time&Window 深度解析](https://www.infoq.cn/article/WCOvi-D68Y8ycCiYZ8pX)
>
> 知乎 [Flink教程-聊聊 flink 1.11 中新的水印策略](https://zhuanlan.zhihu.com/p/158951593)

### 1.介绍

Flink 中 Window 分为时间驱动的（Time Window），和数据驱动的（Count Window）。

![Apache Flink零基础入门（五）：流处理核心组件Time&Window深度解析](https://static001.infoq.cn/resource/image/97/f0/979aacc6b86b42b6fa533e215f26bcf0.jpg)

![Apache Flink零基础入门（五）：流处理核心组件Time&Window深度解析](https://static001.infoq.cn/resource/image/aa/51/aa53afa430834b8d58d4e70d0463a251.jpg)

### 2.Window API

#### 2.1 WindowAssigner

Window 方法接收的输入是一个 WindowAssigner， WindowAssigner 负责将每条输入的数据分发到正确的 Window 中（一条数据可能同时分发到多个 Window 中），Flink 提供了几种通用的 WindowAssigner：tumbling window(窗口间的元素无重复），sliding window（窗口间的元素可能重复），session window 以及 global window。如果需要自己定制数据分发策略，则可以实现一个 class，继承自 WindowAssigner。

#### 2.2 Evictor 

Evictor 主要用于做一些数据的自定义操作，可以在执行用户代码之前，也可以在执行用户代码之后，更详细的描述可以参考 org.apache.flink.streaming.api.windowing.evictors.Evictor 的 evicBefore 和 evicAfter 两个方法。Flink 提供了如下三种通用的 evictor：

- CountEvictor 保留指定数量的元素
- DeltaEvictor 通过执行用户给定的 DeltaFunction 以及预设的 threshold，判断是否删除一个元素。
- TimeEvictor 设定一个阈值 interval，删除所有不再 max_ts - interval 范围内的元素，其中 max_ts 是窗口内时间戳的最大值。

Evictor 是可选的方法，如果用户不选择，则默认没有。

#### 2.3 Trigger

Trigger 用来判断一个窗口是否需要被触发，每个 WindowAssigner 都自带一个默认的 Trigger，如果默认的 Trigger 不能满足你的需求，则可以自定义一个类，继承自 Trigger 即可，我们详细描述下 Trigger 的接口以及含义：

- onElement() 每次往 window 增加一个元素的时候都会触发
- onEventTime() 当 event-time timer 被触发的时候会调用
- onProcessingTime() 当 processing-time timer 被触发的时候会调用
- onMerge() 对两个 trigger 的 state 进行 merge 操作
- clear() window 销毁的时候被调用

上面的接口中前三个会返回一个 TriggerResult，TriggerResult 有如下几种可能的选择：

- CONTINUE 不做任何事情
- FIRE 触发 window
- PURGE 清空整个 window 的元素并销毁窗口
- FIRE_AND_PURGE 触发窗口，然后销毁窗口

#### 2.4 Watermark

在 Flink 中 Time 可以分为三种 Event-Time（事件发生的时间），Processing-Time（处理消息的时间）以及 Ingestion-Time（进入到系统的时间）。

![Apache Flink零基础入门（五）：流处理核心组件Time&Window深度解析](https://static001.infoq.cn/resource/image/c0/9a/c0902e9e20733960b9fd6f980759d09a.jpg)

在 Flink 中通过下面的方式进行 Time 类型的设置：

```java
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
```

通过下面的方式进行 Watermark 的设置（Flink 1.11）：

```java
// Watermark = 进入窗口的最大时间 max_ts - 指定延迟时间 t
input.assignTimestampsAndWatermarks(WatermarkStrategy
                // 指定延迟时间 t (为乱序流生成 Watermark)
                .<Log>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                // 指定事件时间 EventTime 对应的字段
                .withTimestampAssigner(new SerializableTimestampAssigner<Log>() {
                    private static final long serialVersionUID = 5039507823473704463L;

                    @Override
                    public long extractTimestamp(Log element, long recordTimestamp) {

                        return element.timestamp;
                    }
                }));
```

**处理空闲数据源**

在某些情况下，由于数据产生的比较少，导致一段时间内没有数据产生，进而就没有水印的生成，导致下游依赖水印的一些操作就会出现问题，比如某一个算子的上游有多个算子，这种情况下，水印是取其上游两个算子的较小值，如果上游某一个算子因为缺少数据迟迟没有生成水印，就会出现 eventtime 倾斜问题，导致下游没法触发计算。

通过 `WatermarkStrategy.withIdleness()` 方法允许用户在配置的时间内（即超时时间内）没有记录到达时将一个流标记为空闲。这样就意味着下游的数据不需要等待水印的到来。

当下次有水印生成并发射到下游的时候，这个数据流重新变成活跃状态。

```java
WatermarkStrategy
        .<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
        .withIdleness(Duration.ofMinutes(1));
```

#### 2.5 迟到数据

实际工作中我们会使用近似 watermark —— 生成 watermark(t) 之后，还有较小的概率接受到时间戳 t 之前的数据，在 Flink 中将这些数据定义为 “late elements”，同样我们可以在 Window 中指定是允许延迟的最大时间（默认为 0），可以使用下面的代码进行设置：

![Apache Flink零基础入门（五）：流处理核心组件Time&Window深度解析](https://static001.infoq.cn/resource/image/ff/87/ff59255581d4db104eb2c410ab33b887.jpg)

设置 `allowedLateness` 之后，迟来的数据同样可以触发窗口，进行输出，利用 Flink 的 side output 机制，我们可以获取到这些迟到的数据，使用方式如下：

![Apache Flink零基础入门（五）：流处理核心组件Time&Window深度解析](https://static001.infoq.cn/resource/image/90/81/907517a6164aaf6481a201e5d6917381.jpg)

需要注意的是，设置了 allowedLateness 之后，迟到的数据也可能触发窗口，对于 Session window 来说，可能会对窗口进行合并，产生预期外的行为。

