package com.example.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author yi
 * @desciption 测试
 * @date 2020/2/2
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class TestService {

    @Autowired
    RestHighLevelClient client;


    /**
     * 查询数据
     * @author yl
     * @date 2020/2/6 19:52
     * @param
     * @return void
     */
    @Test
    public void get(){
        GetRequest getRequest=new GetRequest("","");
        GetResponse response=null;
        try {
            response=client.get(getRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(response.getSource());
    }

    /**
     * 添加数据
     * @author yl
     * @date 2020/2/6 19:50
     * @param
     * @return void
     */
    @Test
    public void put(){
        IndexRequest request=new IndexRequest("newindex");
        Map map=new HashMap();
        map.put("content","嘿嘿哈哈1");
        request.source(JSONObject.toJSONString(map),XContentType.JSON);
        try {
            IndexResponse index = client.index(request, RequestOptions.DEFAULT);
            index.status().getStatus();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除索引
     * @author yl
     * @date 2020/2/6 19:50
     * @param
     * @return void
     */
    @Test
    public void delIndex(){
        DeleteIndexRequest request=new DeleteIndexRequest("lib");
        try {
            AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除数据
     * @author yl
     * @date 2020/2/6 19:50
     * @param
     * @return void
     */
    @Test
    public void delData(){
        DeleteRequest request=new DeleteRequest("lib");
        request.id("2");
        try {
            client.delete(request,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新文档
     * @author yl
     * @date 2020/2/6 19:54
     * @param
     * @return void
     */
    @Test
    public void update(){
        UpdateRequest request=new UpdateRequest("newindex","Iaz2GXABp98OG4lWdCsS");
        Map map=new HashMap();
        map.put("content","1122");
        request.doc(map);
        try {
            UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createMyltiDoc(){
        List<Object> list = new ArrayList<>();
        Map map1=new HashMap();
        map1.put("content","article1");
        Map map2=new HashMap();
        map2.put("content","article2");
        Map map3=new HashMap();
        map3.put("content","article3");
        list.add(map1);
        list.add(map2);
        list.add(map3);
        createMultiDoc(list);
    }

    /**
     * 批量添加
     * @author yl
     * @date 2020/2/6 20:19
     * @param
     * @return void
     */
    public void createMultiDoc(List<Object> list){
        BulkRequest request=new BulkRequest();

        list.forEach((obj)->{
            request.add(new IndexRequest("newindex").source(JSONObject.toJSONString(obj),XContentType.JSON));
        });
        try {
            BulkResponse bulkResponse=client.bulk(request,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void mget(){
        MultiGetRequest request=new MultiGetRequest();
        request.add(new MultiGetRequest.Item("hashmap","Iqx7GnABp98OG4lWtiuK"));
        request.add(new MultiGetRequest.Item("newindex","1"));
        try {
            MultiGetResponse multiGetResponse=client.mget(request,RequestOptions.DEFAULT);
            System.out.println(multiGetResponse.getResponses().length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询删除
     * @author yl
     * @date 2020/2/6 21:06
     * @param
     * @return void
     */
    @Test
    public void selDel(){
        DeleteByQueryRequest request=new DeleteByQueryRequest("newindex");
        request.setQuery(new TermQueryBuilder("content","1122"));
        try {
            BulkByScrollResponse bulkByScrollResponse = client.deleteByQuery(request, RequestOptions.DEFAULT);
            System.out.println(bulkByScrollResponse.getDeleted());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询所有
     * @author yl
     * @date 2020/2/6 21:13
     * @param
     * @return void
     */
    @Test
    public void selectAll(){
        SearchRequest request=new SearchRequest("newindex");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        request.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            System.out.println(searchResponse.status().getStatus());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void matchQuery(){
        SearchRequest request=new SearchRequest("newindex");
        SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("content","1990-12-12"));
        request.source(sourceBuilder);
        try {
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            System.out.println(searchResponse.getHits().getHits()[0].getSourceAsString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void multiMatchQuery(){
        SearchRequest request=new SearchRequest("newindex");
        SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.multiMatchQuery("JSON","嘿嘿哈哈","{\\\"content\\\":\\\"嘿嘿哈哈\\\"}"));
        request.source(sourceBuilder);
        try {
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            System.out.println(searchResponse.getHits().getHits().length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void term(){
        SearchRequest request=new SearchRequest("newindex");
        SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termsQuery("content","article1","article2"));
        //sourceBuilder.query(QueryBuilders.termQuery("content","article1"));
        request.source(sourceBuilder);
        try {
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            System.out.println(searchResponse.getHits().getHits().length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 范围查询
     * @author yl
     * @date 2020/2/7 20:39
     * @param
     * @return void
     */
    @Test
    public void range(){
        SearchRequest request=new SearchRequest("test_1");
        SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();
        RangeQueryBuilder rangeQueryBuilder=QueryBuilders.rangeQuery("content").from("1993-01-01").to("1997-01-01").format("yyyy-MM-dd");

        BoolQueryBuilder boolQueryBuilder=QueryBuilders.boolQuery();
        boolQueryBuilder.must(rangeQueryBuilder);
        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);
        try {
            SearchResponse search = client.search(request, RequestOptions.DEFAULT);
            SearchHits hits = search.getHits();
            for (SearchHit hit:hits) {
                String sourceAsString = hit.getSourceAsString();
                JSONObject jsonObject = JSON.parseObject(sourceAsString);
                System.out.println(jsonObject);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 前缀查询
     * @author yl
     * @date 2020/2/7 20:39
     * @param
     * @return void
     */
    @Test
    public void prefix(){
        SearchRequest request=new SearchRequest("lib");
        SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();
        PrefixQueryBuilder prefixQueryBuilder=QueryBuilders.prefixQuery("about.keyword","哈");

        BoolQueryBuilder boolQueryBuilder=QueryBuilders.boolQuery();
        boolQueryBuilder.must(prefixQueryBuilder);
        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);
        try {
            SearchResponse search = client.search(request, RequestOptions.DEFAULT);
            SearchHits hits = search.getHits();
            for (SearchHit hit:hits) {
                String sourceAsString = hit.getSourceAsString();
                JSONObject jsonObject = JSON.parseObject(sourceAsString);
                System.out.println(jsonObject);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 通配符查询
     * @author yl
     * @date 2020/2/7 20:40
     * @param 
     * @return void 
     */
    @Test
    public void wildcard(){
        SearchRequest request=new SearchRequest("lib");
        SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();
        WildcardQueryBuilder wildcardQueryBuilder=QueryBuilders.wildcardQuery("about.keyword","*同");

        BoolQueryBuilder boolQueryBuilder=QueryBuilders.boolQuery();
        boolQueryBuilder.must(wildcardQueryBuilder);
        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);
        try {
            SearchResponse search = client.search(request, RequestOptions.DEFAULT);
            SearchHits hits = search.getHits();
            for (SearchHit hit:hits) {
                String sourceAsString = hit.getSourceAsString();
                JSONObject jsonObject = JSON.parseObject(sourceAsString);
                System.out.println(jsonObject);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 模糊查询
     * @author yl
     * @date 2020/2/7 20:46
     * @param
     * @return void
     */
    @Test
    public void fuzzy(){
        SearchRequest request=new SearchRequest("lib");
        SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();
        FuzzyQueryBuilder fuzzyQueryBuilder=QueryBuilders.fuzzyQuery("about","同");

        BoolQueryBuilder boolQueryBuilder=QueryBuilders.boolQuery();
        boolQueryBuilder.must(fuzzyQueryBuilder);
        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);
        try {
            SearchResponse search = client.search(request, RequestOptions.DEFAULT);
            SearchHits hits = search.getHits();
            for (SearchHit hit:hits) {
                String sourceAsString = hit.getSourceAsString();
                JSONObject jsonObject = JSON.parseObject(sourceAsString);
                System.out.println(jsonObject);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * where id in(,)
     * @author yl
     * @date 2020/2/7 20:50
     * @param
     * @return void
     */
    @Test
    public void ids(){
        SearchRequest request=new SearchRequest("newindex");
        SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();
        IdsQueryBuilder idsQueryBuilder=QueryBuilders.idsQuery().addIds("1","H6znGXABp98OG4lWkitO");

        BoolQueryBuilder boolQueryBuilder=QueryBuilders.boolQuery();
        boolQueryBuilder.must(idsQueryBuilder);
        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        request.source(sourceBuilder);
        try {
            SearchResponse search = client.search(request, RequestOptions.DEFAULT);
            SearchHits hits = search.getHits();
            for (SearchHit hit:hits) {
                String sourceAsString = hit.getSourceAsString();
                JSONObject jsonObject = JSON.parseObject(sourceAsString);
                System.out.println(jsonObject);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 聚合函数
     * @author yl
     * @date 2020/2/7 21:02
     * @param
     * @return void
     */
    @Test
    public void agg(){
        SearchRequest request=new SearchRequest("lib");
        SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();
        TermsAggregationBuilder termsAggregationBuilder= AggregationBuilders.terms("by_age").field("age");
        sourceBuilder.aggregation(termsAggregationBuilder);

        sourceBuilder.timeout(new TimeValue(60,TimeUnit.SECONDS));
        request.source(sourceBuilder);
        try {
            SearchResponse search = client.search(request, RequestOptions.DEFAULT);
            Aggregations aggregations=search.getAggregations();
            Map<String, Aggregation> stringAggregationMap = aggregations.asMap();
            ParsedLongTerms by_age = (ParsedLongTerms) stringAggregationMap.get("by_age");
            List<? extends Terms.Bucket> buckets = by_age.getBuckets();
            for (Terms.Bucket bucket:buckets) {
                System.out.println(bucket.getDocCount()+"\t"+bucket.getKeyAsNumber());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 组合
     * @author yl
     * @date 2020/2/8 12:02
     * @param
     * @return void
     */
    @Test
    public void zuhe(){
        SearchRequest searchRequest = new SearchRequest("test_es");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //如果用name直接查询，其实是匹配name分词过后的索引查到的记录(倒排索引)；如果用name.keyword查询则是不分词的查询，正常查询到的记录
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("birthday").from("1991-01-01").to("2010-10-10").format("yyyy-MM-dd");//范围查询
        PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery("name.keyword", "张");//前缀查询
        FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort("age");//按照年龄排序
        fieldSortBuilder.sortMode(SortMode.MIN);//从小到大排序

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(rangeQueryBuilder).should(prefixQueryBuilder);//and or  查询

        sourceBuilder.query(boolQueryBuilder).sort(fieldSortBuilder);//多条件查询
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
