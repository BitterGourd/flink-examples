package com.github.connectors.filesystem;

import com.github.connectors.pojo.Student;
import com.github.connectors.util.StudentGenerator;
import com.github.connectors.util.StudentVectorizer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

/**
 * 官方文档：https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/connectors/streamfile_sink.html
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
public class FsBulkFormatSink {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(5000L);
        env.setStateBackend(new FsStateBackend(params.get("checkpoint", "file:///checkpoint")));

        DataStream<Student> input = env.addSource(new StudentGenerator(100L, 1000, 150.0D));

        input.print();

        final String output = params.get("output", "file:///output");
        final String schema = "struct<_col0:int,_col1:string,_col2:double>";
        Properties writerProps = new Properties();
        writerProps.setProperty("orc.compress", "LZ4");
        final OrcBulkWriterFactory<Student> writerFactory = new OrcBulkWriterFactory<>(
                new StudentVectorizer(schema), writerProps, new Configuration());
        // TODO 有问题，in-progress 文件一直增加，数据没写入文件，文件也未滚动
        final StreamingFileSink<Student> sink = StreamingFileSink
                .<Student>forBulkFormat(new Path(output), writerFactory)
                // 仅在每个检查点上滚动
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        input.addSink(sink);

        env.execute("Flink Connector Filesystem");
    }
}
