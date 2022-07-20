package com.study.sink;

import com.study.base.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author osmondy
 */
public class SinkToEs {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟一个 StreamSource
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));


        // 创建一个ElasticsearchSinkFunction
        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction  = new ElasticsearchSinkFunction<Event>() {
            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                //将需要写入ES的字段依次添加到Map当中
                HashMap<String, String> data = new HashMap<>();
                data.put(event.user, event.url);

                IndexRequest source = Requests.indexRequest()
                        .index("clicks")
                        .type("type")
                        .source(data);

                requestIndexer.add(source);
            }
        };

        // 定义es的连接配置  带用户名密码
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("172.16.9.89", 9200, "http"));
        httpHosts.add(new HttpHost("172.16.9.89", 9201, "http"));
        httpHosts.add(new HttpHost("172.16.9.89", 9202, "http"));

        ElasticsearchSink.Builder<Event> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                elasticsearchSinkFunction);
        //设置用户名密码
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic","123456"));
                            return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    });
                }
        );

        stream.addSink(esSinkBuilder.build());
        env.execute();

    }
}