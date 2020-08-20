package com.github.streaming.assignuid;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Savepoint 文档：https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/ops/state/savepoints.html
 *
 * 对于有状态的 Flink 应用，推荐给每个算子都指定唯一用户ID (UUID)
 * 严格地说，仅需要给有状态的算子设置就足够了
 * 但是因为 Flink 的某些内置算子 (如 window) 是有状态的，而有些是无状态的，可能用户不是很清楚哪些内置算子是有状态的，哪些不是
 * 所以从实践经验上来说，建议每个算子都指定上 UUID
 * 
 * 如果不手动指定 ID ，则会自动生成 ID
 * 只要这些 ID 不变，就可以从 Savepoint 自动恢复
 * 生成的 ID 取决于程序的结构，并且对程序更改很敏感
 * 因此，强烈建议手动分配这些 ID
 */
public class AssignUid {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*
         * DataStream<String> stream = env.
         *      // Stateful source (e.g. Kafka) with ID
         *      .addSource(new StatefulSource())
         *      .uid("source-id") // ID for the source operator
         *      .shuffle()
         *      // Stateful mapper with ID
         *      .map(new StatefulMapper())
         *      .uid("mapper-id") // ID for the mapper
         *      // Stateless printing sink
         *      .print(); // Auto-generated ID
         *
         * 可以将 Savepoint 想象为每个有状态的算子保存一个映射 "算子 ID -> 状态" :
         *      Operator ID | State
         *      ------------+------------------------
         *      source-id   | State of StatefulSource
         *      mapper-id   | State of StatefulMapper
         *
         * 在上面的示例中，print sink 是无状态的，因此不是 Savepoint 状态的一部分
         * 默认情况下，我们尝试将 Savepoint 的每个条目映射回新程序
         */
        env.fromElements(1, 2, 3, 4 ,5, 6).uid("source")
                .filter(num -> num % 2 == 0).uid("filter even")
                .print().uid("sink");

        env.execute("Flink Streaming Setting Uid");
    }
}
