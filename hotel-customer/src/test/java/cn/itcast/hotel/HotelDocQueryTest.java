package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
public class HotelDocQueryTest {
    private RestHighLevelClient restHighLevelClient;

    @BeforeEach
    void setUp() {
        this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.154.138:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.restHighLevelClient.close();
    }

    private void handleResponse(SearchRequest request) throws IOException {
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
//        System.out.println(response);
        SearchHits searchHitsHits = response.getHits();
        long value = searchHitsHits.getTotalHits().value;
        System.out.println("total hits value: " + value);
        SearchHit[] hits = searchHitsHits.getHits();
        for (SearchHit hit : hits) {
            String jsonStr = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(jsonStr, HotelDoc.class);
            System.out.println(hotelDoc);
        }
    }

    @Test
//    test matchAll
    void testMatchAll() throws IOException {
//        1. create request
        SearchRequest request = new SearchRequest("hotel");
//        2. prepare DSL
        request.source().query(QueryBuilders.matchAllQuery());
//        3. send request
        handleResponse(request);
    }

    @Test
//    test match query
    void testMatch() throws IOException {
        //        1. create request
        SearchRequest request = new SearchRequest("hotel");
//        2. prepare DSL
        request.source().query(QueryBuilders.matchQuery("all", "如家"));
//        3. send request
        handleResponse(request);
    }

    @Test
//    test term query
    void testTerm() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.termQuery("city", "杭州"));
        handleResponse(request);
    }

    @Test
//    test range query
    void testRange() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.rangeQuery("price").gte(250).lte(1000));
        handleResponse(request);
    }

    @Test
//    test boolean query
    void testBoolean() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        BoolQueryBuilder booleanQuery = QueryBuilders.boolQuery();
        booleanQuery.must(QueryBuilders.termQuery("city", "杭州"));
        booleanQuery.filter(QueryBuilders.rangeQuery("price").lte(250));
        request.source().query(booleanQuery);
        handleResponse(request);
    }

    @Test
//    test sort_page query
    void testSort_Page() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source()
                .query(QueryBuilders.matchAllQuery())
                .from(0)
                .size(20)
                .sort("price", SortOrder.DESC);
        handleResponse(request);
    }

    @Test
    void testHighlight() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source()
                .query(QueryBuilders.matchQuery("all", "如家"))
                .highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        handleResponse(request);
    }

}
