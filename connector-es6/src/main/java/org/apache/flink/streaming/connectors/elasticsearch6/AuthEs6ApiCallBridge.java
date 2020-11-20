package org.apache.flink.streaming.connectors.elasticsearch6;

//import org.apache.flink.streaming.connectors.elasticsearch6.Elasticsearch6ApiCallBridge;
//import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.http.HttpHost;
//import org.elasticsearch.client.RestClient;
//import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;
import java.util.Map;

/**
 * @program: slink
 * @author: wuren
 * @create: 2020/11/11
 **/
public class AuthEs6ApiCallBridge extends Elasticsearch6ApiCallBridge {

    AuthEs6ApiCallBridge(List<HttpHost> httpHosts, RestClientFactory restClientFactory) {
        super(httpHosts, restClientFactory);
    }
    @Override
    public RestHighLevelClient createClient(Map<String, String> clientConfig) {
//        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]));
//        restClientFactory.configureRestClientBuilder(builder);
//
//        RestHighLevelClient rhlClient = new RestHighLevelClient(builder);
//
//        return rhlClient;
        return null;
    }
}
