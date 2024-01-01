package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParam;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private PageResult handleResponse(SearchRequest request) throws IOException {
        SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
//        System.out.println(response);

        SearchHits searchHitsHits = response.getHits();
        long total = searchHitsHits.getTotalHits().value;
//        System.out.println("total hits value: " + total);
        SearchHit[] hits = searchHitsHits.getHits();

        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            String jsonStr = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(jsonStr, HotelDoc.class);
//            System.out.println(hotelDoc);
            hotels.add(hotelDoc);
        }
        return new PageResult(total, hotels);
    }

    public PageResult search(RequestParam requestParam) {
        try {
            SearchRequest request = new SearchRequest("hotel");

//            original boolean query here
            BoolQueryBuilder boolQuery = buildBasicQuery(requestParam);

            FunctionScoreQueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery(
//                original boolean query above
                    boolQuery,
//                function score query
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
//                        filter
                            new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                    QueryBuilders.termQuery("isAD", true),
//                            weight function
                                    ScoreFunctionBuilders.weightFactorFunction(10)
                            )
                    });

            Integer page = requestParam.getPage();
            Integer size = requestParam.getSize();
            request.source()
                    .query(functionScoreQuery)
                    .from((page-1)*size)
                    .size(size);

            String location = requestParam.getLocation();
            if (location != null && !location.equals("")) {
                request.source().sort(
                        SortBuilders.geoDistanceSort("location", new GeoPoint(location))
                                .order(SortOrder.ASC)
                                .unit(DistanceUnit.KILOMETERS)
                );
            }

            return handleResponse(request);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public Map<String, List<String>> filters(RequestParam requestParam) throws IOException {
        try {
            SearchRequest request = new SearchRequest("hotel");

            buildBasicQuery(requestParam);

            request.source().size(0);
//        build brand, city, star aggregations
            buildAgg(request);
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);

//        a map for storing brand, city and star lists
            Map<String, List<String>> result = new HashMap<>();

//        here the agg contains 3 aspects
            Aggregations aggregations = response.getAggregations();

//        brand bucket, city bucket and star bucket
            List<String> brandList = getAggByName(aggregations, "brandAgg");
            List<String> cityList = getAggByName(aggregations, "cityAgg");
            List<String> starList = getAggByName(aggregations, "starAgg");

            result.put("brand", brandList);
            result.put("city", cityList);
            result.put("star", starList);

            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSuggestions(String prefix) throws IOException {
        try {
//        prepare request
            SearchRequest request = new SearchRequest("hotel");
//        request DSL
            request.source()
                    .suggest(new SuggestBuilder().addSuggestion(
                            "mySuggestion",
                            SuggestBuilders
                                    .completionSuggestion("suggestion")
                                    .prefix("sh")
                                    .skipDuplicates(true)
                                    .size(10)
                    ));
//        send request, get response
            SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
//        result list
            List<String> suggestions_list = new ArrayList<>();

            Suggest suggests = response.getSuggest();
            CompletionSuggestion suggestions = suggests.getSuggestion("mySuggestion");
            for (CompletionSuggestion.Entry.Option option : suggestions.getOptions()) {
                String text = option.getText().toString();
                suggestions_list.add(text);
            }
            return suggestions_list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertById(Long id) {
        try {
            Hotel hotel = getById(id);
            HotelDoc hotelDoc = new HotelDoc(hotel);

            IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
            request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            restHighLevelClient.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteById(Long id) {
        try {
            DeleteRequest request = new DeleteRequest("hotel", id.toString());
            restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static List<String> getAggByName(Aggregations aggregations, String aggName) {
        Terms terms = aggregations.get(aggName);
        if (terms == null) {
            return new ArrayList<>();
        }
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        List<String > List = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            String Name = bucket.getKeyAsString();
            List.add(Name);
        }
        return List;
    }

    private static void buildAgg(SearchRequest request) {
        //        brand agg
        request.source()
                .aggregation(
                        AggregationBuilders
                                .terms("brandAgg")
                                .field("brand")
                                .size(10)
                );
//        city agg
        request.source()
                .aggregation(
                        AggregationBuilders
                                .terms("cityAgg")
                                .field("city")
                                .size(10)
                );
//        star agg
        request.source()
                .aggregation(
                        AggregationBuilders
                                .terms("starAgg")
                                .field("starName.keyword")
                                .size(10)
                );
    }

    private static BoolQueryBuilder buildBasicQuery(RequestParam requestParam) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        String key = requestParam.getKey();
        if (key == null || "".equals(key)){
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }

        if (requestParam.getCity() != null && !"".equals(requestParam.getCity())) {
            boolQuery.filter(QueryBuilders.termQuery("city", requestParam.getCity()));
        }

        if (requestParam.getBrand() != null && !"".equals(requestParam.getBrand())) {
            boolQuery.filter(QueryBuilders.termQuery("brand", requestParam.getBrand()));
        }

        if (requestParam.getStarName() != null && !"".equals(requestParam.getStarName())) {
            boolQuery.filter(QueryBuilders.termQuery("starName", requestParam.getStarName()));
        }

        if (requestParam.getMinPrice() != null && requestParam.getMaxPrice() != null) {
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(requestParam.getMinPrice()).lte(requestParam.getMaxPrice()));
        }
        return boolQuery;
    }

}
