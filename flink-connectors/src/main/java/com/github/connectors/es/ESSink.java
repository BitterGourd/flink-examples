package com.github.connectors.es;

import com.github.connectors.pojo.Student;
import com.github.connectors.util.StudentGenerator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * flink-connector-elasticsearch 官方文档：https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/connectors/elasticsearch.html
 *
 * 版本适配：
 *  flink-connector-elasticsearch5_2.11 =>  ES 5.x
 *  flink-connector-elasticsearch6_2.11 =>  ES 6.x
 *  flink-connector-elasticsearch7_2.11 =>  ES 7 and later versions
 */
public class ESSink {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启 Checkpoint，可以保证 at-least-once 语义
        env.enableCheckpointing(5000L);

        DataStream<Student> input = env.addSource(new StudentGenerator(100L, 1000, 150.0D));

        input.print();

        // TODO ES 6.7.0 测试时未写入成功
        List<HttpHost> httpHosts = Collections.singletonList(new HttpHost("127.0.0.1", 9200, "http"));
        final ElasticsearchSink.Builder<Student> esSinkBuilder = new ElasticsearchSink.Builder<Student>(
                httpHosts,
                new ElasticsearchSinkFunction<Student>() {
                    private static final long serialVersionUID = 6337634330353081773L;

                    @Override
                    public void process(Student element, RuntimeContext ctx, RequestIndexer indexer) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element.toString());
                        IndexRequest request = Requests.indexRequest()
                                .index("student")
                                .source(json);

                        indexer.add(request);
                    }
                });

        // 设置批量请求个数，这里设置为 1，每个请求都会直接发送，不会缓存起来
        esSinkBuilder.setBulkFlushMaxActions(1);

        // 为 REST client 自定义配置
        // esSinkBuilder.setRestClientFactory(
        //         restClientBuilder -> {
        //             restClientBuilder.setDefaultHeaders(...)
        //             restClientBuilder.setMaxRetryTimeoutMillis(...)
        //             restClientBuilder.setPathPrefix(...)
        //             restClientBuilder.setHttpClientConfigCallback(...)
        //         }
        // );


        /* ---------------------------------------------------------------------------------------------------------- *\
         * 使用 ElasticsearchSink 构造方法可以指定 ActionRequestFailureHandler 来处理请求失败
         * 重要说明：失败时，将请求重新添加到内部BulkProcessor会导致更长的检查点，因为接收器在检查点时还需要等待刷新的新请求被刷新
         *          例如，当使用RetryRejectedExecutionFailureHandler时，检查点将需要等待，
         *          直到Elasticsearch节点队列具有足够的容量来处理所有挂起的请求。
         *          这也意味着，如果重新添加的请求永远不会成功，则检查点将永远不会完成。
         *
         * bulkRequestsConfig 配置：
         *  1.缓冲与刷新操作请求
         *      bulk.flush.max.actions: 刷新前的最大缓冲操作数
         *      bulk.flush.max.size.mb: 刷新前的最大缓冲数据大小(单位 MB)
         *      bulk.flush.interval.ms: 无论缓冲操作的数量或大小如何，刷新间隔(毫秒)
         *  2.临时请求错误重试
         *      bulk.flush.backoff.enable: 如果由于临时 EsRejectedExecutionException 而导致其一个或多个操作失败，是否对刷新执行 backoff delay
         *      bulk.flush.backoff.type: backoff delay 的类型，常量(CONSTANT)或指数(EXPONENTIAL)
         *      bulk.flush.backoff.delay: backoff 的延迟，常量类型的 backoff 指每次重试之间的延迟，指数类型的 backoff 指初始基准延迟
         *      bulk.flush.backoff.retries: backoff 重试的次数
         * ---------------------------------------------------------------------------------------------------------- */
         /*
         input.addSink(new ElasticsearchSink<>(
                bulkRequestsConfig, httpHosts,
                new ElasticsearchSinkFunction<String>() {...},
                new ActionRequestFailureHandler() {
                    @Override
                    void onFailure(ActionRequest action,
                                   Throwable failure,
                                   int restStatusCode,
                                   RequestIndexer indexer) throw Throwable {

                        if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                            // 重新添加由于队列容量饱和而失败的请求
                            indexer.add(action);
                        } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
                            // 丢弃格式错误的文档的请求
                        } else {
                            // for all other failures, fail the sink
                            // here the failure is simply rethrown, but users can also choose to throw custom exceptions
                            throw failure;
                        }
                    }
                }, restClientFactory));
         */


        input.addSink(esSinkBuilder.build());

        env.execute("Flink Connector Elasticsearch");
    }
}
