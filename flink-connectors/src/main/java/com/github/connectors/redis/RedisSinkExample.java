package com.github.connectors.redis;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * flink-connector-redis 文档：https://bahir.apache.org/docs/flink/current/flink-streaming-redis/
 *
 * 由 Apache Bahir 发布，redis 版本为 2.8.5，测试 5.0.x 兼容
 */
public class RedisSinkExample {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, String>> source = env.fromElements(
                new Tuple2<>("FLINK", "FLINK"),
                new Tuple2<>("SPARK", "SPARK"),
                new Tuple2<>("STORM", "STORM")
        );

        // 单机 redis
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost(params.get("host", "127.0.0.1")).build();
        // redis 集群
        // FlinkJedisClusterConfig conf = new FlinkJedisClusterConfig.Builder()
        //         .setNodes(new HashSet<InetSocketAddress>(Arrays.asList(
        //                 new InetSocketAddress("localhost", 5601))))
        //         .build();
        // redis 哨兵
        // FlinkJedisSentinelConfig conf = new FlinkJedisSentinelConfig.Builder()
        //         .setMasterName("master").setSentinels(new HashSet<>(Arrays.asList(
        //                 "localhost:26379")))
        //         .build();
        source.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisExampleMapper()));

        env.execute("Flink Connector Redis");
    }

    public static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>> {

        private static final long serialVersionUID = 1072318553238196234L;

        @Override
        public RedisCommandDescription getCommandDescription() {
            // RedisCommand 对应 redis 各数据类型的操作
            return new RedisCommandDescription(RedisCommand.HSET, "FLINK_REDIS_TEST");
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}
