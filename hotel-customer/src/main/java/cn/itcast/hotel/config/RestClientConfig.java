package cn.itcast.hotel.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RestClientConfig {
    @Bean
    public RestHighLevelClient register() {
        return new RestHighLevelClient(org.elasticsearch.client.RestClient.builder(
                HttpHost.create("http://192.168.154.138:9200")
        ));
    };
}
