package com.github.connectors.filesystem;

import com.github.connectors.pojo.Student;
import com.github.connectors.util.StudentGenerator;
import org.apache.commons.io.Charsets;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

/**
 * flink-connector-filesystem 官方文档：https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/connectors/streamfile_sink.html
 *
 * 重要：使用 StreamingFileSink 时需要启用 Checkpoint ，每次做 Checkpoint 时写入完成
 *      如果 Checkpoint 被禁用，part file 将永远处于 'in-progress' 或 'pending' 状态，下游系统无法安全地读取
 *
 * part file 生命周期：
 *  1.In-progress ：当前文件正在写入中
 *  2.Pending ：当处于 In-progress 状态的文件关闭（closed）了，就变为 Pending 状态
 *  3.Finished ：在成功的 Checkpoint 后，Pending 状态将变为 Finished 状态
 *      处于 Finished 状态的文件不会再被修改，可以被下游系统安全地读取
 */
public class FsRowFormatSink {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(3000L);

        DataStream<Student> input = env.addSource(new StudentGenerator(100L, 1000, 150.0D));

        final String output = params.get("output", "file:///output");
        final StreamingFileSink<String> sink = StreamingFileSink
                .<String>forRowFormat(new Path(output), new SimpleStringEncoder<String>(Charsets.UTF_8.name()))
                // 指定滚动策略，在以下三种情况下滚动处于 In-progress 状态的 part file (便于测试，都调得很小)：
                //  1.至少包含 15 秒钟的数据
                //  2.最近 5 秒钟没有收到新的记录
                //  3.文件大小达到 2KB （写入最后一条记录后）
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(15))
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(5))
                                .withMaxPartSize(1024 * 2)
                                .build())
                // 记录分配时间桶规则，默认分配给一小时时间桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH", ZoneId.systemDefault()))
                // 毫秒间隔，用于基于时间的滚动策略。默认 1 分钟
                .withBucketCheckInterval(60L * 1000L)
                // 同时指定 BucketAssigner 和 RollingPolicy
                // .withNewBucketAssignerAndPolicy(bucketAssigner, rollingPolicy)
                // 指定 part file 配置(前缀、后缀)
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("part").withPartSuffix("").build())
                .build();

        input.map(Student::toString).addSink(sink);

        env.execute("Flink Connector Filesystem");
    }
}
