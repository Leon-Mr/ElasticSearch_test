package com.example.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
}
