package com.github.streaming.checkpointing;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 默认情况下 checkpoint 是禁用的。
 * 通过调用 StreamExecutionEnvironment 的 enableCheckpointing(n) 来启用 checkpoint，
 * 里面的 n 是进行 checkpoint 的间隔，单位毫秒
 *
 * 官方文档：https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/stream/state/checkpointing.html
 */
public class Checkpointing {

    public static void main(String[] args) throws IOException {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 每 1000ms 开始一次 checkpoint
        env.enableCheckpointing(1000L);

        // 设置模式为精确一次 (这是默认值)
        // 或者使用 env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置两次 checkpoint attempts 之间的最小暂停时间为 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);

        // checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        // 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 开启在 job 中止后仍然保留的 externalized checkpoints
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 允许在有更新状态的 savepoint 时回退到 checkpoint，默认 false
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        // ==============================================================
        /*
         * 启动 checkpoint 机制时，状态会随着 checkpoint 而持久化，以防止数据丢失、保障恢复时的一致性
         * 状态内部的存储格式、状态在 checkpoint 时如何持久化以及持久化在哪里均取决于选择的 State Backend
         * 如果不设置，默认使用 MemoryStateBackend
         *
         * 官方同时建议将 managed memory 设为 0，以保证将最大限度的内存分配给 JVM 上的用户代码
         */
        env.setStateBackend(new FsStateBackend(
                "hdfs://namenode:40010/flink/checkpoints",
                // 使用异步快照来防止 checkpoint 写状态时对数据处理造成阻塞，默认为 true (MemoryStateBackend 同样)
                true));

        // RocksDBStateBackend 只支持异步快照
        env.setStateBackend(new RocksDBStateBackend(
                "hdfs://namenode:40010/flink/checkpoints",
                // 开启增量快照
                true
        ));


        // ==============================================================
        // 设置本作页重启策略为固定延迟重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                // 尝试重启的次数
                3,
                // 延时
                Time.of(10, TimeUnit.SECONDS)
        ));

        // 设置本作页重启策略为故障率重启，当故障率（每个时间间隔发生故障的次数）超过设定的限制时，作业会最终失败
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                // 每个时间间隔的最大故障次数
                3,
                // 测量故障率的时间间隔
                Time.of(5, TimeUnit.SECONDS),
                // 延时
                Time.of(10, TimeUnit.SECONDS)
        ));

        // 使用群集定义的重启策略，这对于启用了 checkpoint 的流处理程序很有帮助，如果没有定义其他重启策略，默认选择固定延时重启策略
        env.setRestartStrategy(RestartStrategies.fallBackRestart());
    }
}
