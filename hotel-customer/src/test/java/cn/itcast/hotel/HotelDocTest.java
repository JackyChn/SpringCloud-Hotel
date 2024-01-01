package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.apache.http.client.protocol.RequestClientConnControl;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
public class HotelDocTest {

    @Autowired
    private IHotelService hotelService;
    private RestHighLevelClient restHighLevelClient;

    @BeforeEach
    void setUp() {
        this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.154.138:9200")
        ));
    }

    @AfterEach
    void tesrDown() throws IOException {
        this.restHighLevelClient.close();
    }

    @Test
    void testAddDoc() throws IOException {
//      get hotel info by id, transfer to hotelDoc due to location
        Hotel hotel = hotelService.getById(61083L);
        HotelDoc hotelDoc = new HotelDoc(hotel);

//        1.prepare doc object, using toString() to turn long type to string type
        IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());

//        2.prepare json object, turn that hotelDoc java object to json format
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);

//        3.send request
        restHighLevelClient.index(request, RequestOptions.DEFAULT);

    }

    @Test
    void testGetDoc() throws IOException {
        GetRequest request = new GetRequest("hotel", "61083");
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        String json = response.getSourceAsString();
        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    @Test
    void testUpdateDoc() throws IOException {
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        request.doc(
                "price", "952",
                "starName", "四钻"
        );
        restHighLevelClient.update(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteDoc() throws IOException {
        DeleteRequest request = new DeleteRequest("hotel", "61083");
        restHighLevelClient.delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void testBulkRequest() throws IOException {
//        bulk search info from db
        List<Hotel> hotels = hotelService.list();

//        bulk request
        BulkRequest request = new BulkRequest();

//        turn that list to hotelDoc
        for (Hotel hotel : hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(new IndexRequest("hotel")
                    .id(hotel.getId().toString())
                    .source(JSON.toJSONString(hotelDoc), XContentType.JSON));
        }
        restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
    }
}
