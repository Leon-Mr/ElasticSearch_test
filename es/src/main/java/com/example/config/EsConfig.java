package com.example.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author yi
 * @desciption es配置类
 * @date 2020/2/2
 */
@Configuration
public class EsConfig {

    @Value("${es.host}")
    private String host;
    @Value("${es.port}")
    private Integer port;
    @Value("${es.scheme}")
    private String scheme;

    @Bean
    public RestHighLevelClient client(){
        return new RestHighLevelClient(RestClient.builder(new HttpHost(host,port,scheme)));
    }
}
