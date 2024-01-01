package cn.itcast.hotel;

import cn.itcast.hotel.service.IHotelService;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
public class HotelDocAutoFillTest {
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
    public void testAutoFill() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source()
                .suggest(new SuggestBuilder().addSuggestion(
                        "mySuggestion",
                        SuggestBuilders
                                .completionSuggestion("suggestion")
                                .prefix("sh")
                                .skipDuplicates(true)
                                .size(10)
                ));
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
        Suggest suggests = response.getSuggest();
        CompletionSuggestion suggestions = suggests.getSuggestion("mySuggestion");
        for (CompletionSuggestion.Entry.Option option : suggestions.getOptions()) {
            String text = option.getText().toString();
            System.out.println(text);
        }
    }
}
