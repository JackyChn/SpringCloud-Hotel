package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelIndexTest {
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

    @Test
//    test create index
    void testCreateHotelIndex() throws IOException {
//        1.create request object
        CreateIndexRequest request = new CreateIndexRequest("hotel");

//        2.params, MAPPING_TEMPLATE is static const string, content is DSL
        request.source(MAPPING_TEMPLATE, XContentType.JSON);

//        3.send request
        restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void testGetHotelIndex() throws IOException {
//        1.request
        GetIndexRequest request = new GetIndexRequest("hotel");

//        2.get response
        GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(request, RequestOptions.DEFAULT);

        String[] indices = getIndexResponse.getIndices();
        for (String index : indices) {
            System.out.println("Index: " + index);
            System.out.println("Settings: " + getIndexResponse.getSettings().get(index).toString());
            System.out.println("Mappings: " + getIndexResponse.getMappings().get(index).toString());
        }
    }

    @Test
    void testDeleteHotelIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void testExistHotelIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("hotel");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        if(exists) {
            System.out.println("index exists");
        } else {
            System.out.println("index not exists");
        }
    }
}
