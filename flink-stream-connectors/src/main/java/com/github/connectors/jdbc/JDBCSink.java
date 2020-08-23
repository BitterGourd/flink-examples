package com.github.connectors.jdbc;

import com.github.connectors.pojo.Student;
import com.github.connectors.util.StudentGenerator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * flink-connector-jdbc 官方文档：https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/connectors/jdbc.html
 *
 * flink-connector-jdbc 可以保证 at-least-once，使用 upsert 或幂等更新可以支持 exactly-once
 *
 * 指定参数 interval-ms=0 可以观察不设置批次间隔(或为 0)时批次是何时刷写数据库的，
 * 指定参数 interval-ms>0 可以观察设置了批次间隔后批次是何时刷写数据库的
 */
public class JDBCSink {

    public static void main(String[] args) throws Exception {

        System.out.println("Application Usage: JDBCSink --jdbc-driver <jdbc-driver> \\");
        System.out.println("--jdbc-url <jdbc-url> --jdbc-user <jdbc-user> --jdbc-password <jdbc-password>");
        System.out.println("[--parallelism <parallelism>] [--batch-size <batch-size>] [--interval-ms <interval-ms>]\n");

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(params.getInt("parallelism", 1));

        /*DataStream<Student> source = env.fromElements(
                Student.of(1, "Bob", 85.0D),
                Student.of(2, "Lucy", 90.0D),
                Student.of(3, "James", 150.0D)
        );*/
        DataStream<Student> source = env.addSource(new StudentGenerator(2000L, 10000, 150.0D));

        source.print();

        final String driverName = params.get("jdbc-driver", "com.mysql.jdbc.Driver");
        final String jdbcUrl = params.get("jdbc-url", "jdbc:mysql://localhost:3306/test");
        final String jdbcUserName = params.get("jdbc-user", "root");
        final String jdbcPassword = params.get("jdbc-password", "root");
        final int batchSize = params.getInt("batch-size", 10);
        final long intervalMs = params.getLong("interval-ms", 10000L);
        /*
            CREATE TABLE `student` (
              `user_id` int(11) UNSIGNED COMMENT '学生ID',
              `name` varchar(32) COMMENT '学生姓名',
              `score` double COMMENT '学生成绩',
              PRIMARY KEY ( `user_id` )
            )
         */
        source.addSink(JdbcSink.sink(
                "INSERT INTO student(user_id,name,score) VALUES(?,?,?) ON DUPLICATE KEY UPDATE name=?,score=?",
                (ps, stu) -> {
                    ps.setInt(1, stu.getUserId());
                    ps.setString(2, stu.getName());
                    ps.setDouble(3, stu.getScore());
                    ps.setString(4, stu.getName());
                    ps.setDouble(5, stu.getScore());
                },
                // JDBC 批量插入配置，默认 5000 条数据一批，每批插入时间间隔 0 ms(凑不足 batchSize 数据会一直等待)，失败重试 3 次
                // 设置批次间隔后，如果数据条数没有达到 batchSize，但间隔时间到了，同样会将缓冲区数据刷入数据库(所有分区)
                // 注意(坑)：如果并行度大于 1， batchSize 是针对各分区而言的，不是针对整个 source，所以建议同时指定 batchInterval
                JdbcExecutionOptions.builder().withBatchSize(batchSize).withBatchIntervalMs(intervalMs).withMaxRetries(3).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(driverName)
                        .withUrl(jdbcUrl)
                        .withUsername(jdbcUserName)
                        .withPassword(jdbcPassword)
                        .build()
        ));

        env.execute("Flink Connector JDBC");
    }
}
