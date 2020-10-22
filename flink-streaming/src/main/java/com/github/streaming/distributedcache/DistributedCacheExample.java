package com.github.streaming.distributedcache;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Flink 分布式缓存 Distributed Cache
 * 程序运行时，Flink 会自动将缓存中的文件或目录复制到所有 worker 节点的本地系统，然后从本地系统访问它
 */
public class DistributedCacheExample {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);

        String cacheFile = params.get("cacheFile-path", "file:///D:/Project/Java/flink-examples/flink-streaming" +
                "/src/main/resources/transactions.csv");
        env.registerCachedFile(cacheFile, "transactions.csv");

        DataStream<Integer> source = env.fromElements(1, 2, 3, 4, 5);

        source.map(new RichMapFunction<Integer, String>() {

            private Map<Integer, String> map = new HashMap<>(10);

            @Override
            public void open(Configuration parameters) throws Exception {
                File cachedFile = getRuntimeContext().getDistributedCache().getFile("transactions.csv");

                try (BufferedReader reader = new BufferedReader(new FileReader(cachedFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] split = line.split(",");
                        map.put(Integer.parseInt(split[0]), split[1]);
                    }
                }
            }

            @Override
            public String map(Integer value) throws Exception {
                return value + " -> " + map.getOrDefault(value, "");
            }
        }).print();

        env.execute("Flink Distributed Cache.");
    }
}
