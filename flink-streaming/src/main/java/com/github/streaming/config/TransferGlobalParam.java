package com.github.streaming.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class TransferGlobalParam {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        // 从文件获取配置
        // env.getConfig().setGlobalJobParameters(ParameterTool.fromPropertiesFile(TransferGlobalParam.class.getResourceAsStream("application.properties")));

        env.addSource(new RichSourceFunction<String>() {
            private static final long serialVersionUID = -3222116349208258884L;
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                /* 不止局限在 RichSourceFunction 使用，可以在任意 RichFunction 中使用 */
                ParameterTool parameterTool =
                        (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                while (isRunning) {
                    ctx.collect(parameterTool.get("topic", "configTest") + " -> " + System.currentTimeMillis());
                    Thread.sleep(500);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).print();

        env.execute("Flink Streaming Configuration Transfer");
    }
}
