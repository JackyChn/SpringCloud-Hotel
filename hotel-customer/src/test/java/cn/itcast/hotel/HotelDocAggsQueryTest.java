package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.RequestParam;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@SpringBootTest
public class HotelDocAggsQueryTest {
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private IHotelService iHotelService;

    @BeforeEach
    public void setUp() {
        this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.154.138:9200")
        ));
    }

    @AfterEach
    public void tearDown() throws IOException {
        this.restHighLevelClient.close();
    }

    @Test
//    test bucket aggs
    public void testBucket() throws IOException {
//        1. request
        SearchRequest request = new SearchRequest("hotel");
//        2. DSL
        request.source()
                .size(0)
                .aggregation(
                        AggregationBuilders
                                .terms("brandAgg")
                                .field("brand")
                                .size(20)
                );
        handleResponse(request);
    }

//    @Test
//    public void testMultiAggQuery() throws IOException {
//        Map<String, List<String>> filters = iHotelService.filters();
//        System.out.println(filters);
//    }

    private void handleResponse(SearchRequest request) throws IOException {
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
//        System.out.println(response);
        Aggregations aggregations = response.getAggregations();
        Terms brandTerms = aggregations.get("brandAgg");
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String brandName = bucket.getKeyAsString();
            System.out.println(brandName);
        }
    }
}
