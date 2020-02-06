package com.example.demo;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.RequestBuilder;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;

import lombok.val;

/**
 * @author jh
 * @desciption es测试
 * @date 2020/2/2
 */
public class EsDemo {

    // 从es中查询数据
    @Test
    public void test1() throws Exception {
        // 指定ES集群
//		Settings setting = Settings.builder().put("cluster.name","docker-cluster").build();

        // 创建访问es服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("47.113.192.16", 9200, "http")));
        GetRequest request = new GetRequest("lib", "1");
        // 数据查询
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 得到查询出的数据
        System.err.println(response.getSourceAsString());
        client.close();
    }

    // 添加文档
    @Test
    public void test2() throws Exception {
        // 创建访问es服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("47.113.192.16", 9200, "http")));
        // 添加文档
        /*
         * PUT /index1 { "settings": { "number_of_shards": 3, "number_of_replicas": 0 },
         * "mappings": { "properties": { "id": { "type": "long" }, "title": { "type":
         * "text", "analyzer": "ik_max_word" }, "content": { "type": "text", "analyzer":
         * "ik_max_word" }, "postdate": { "type": "date" }, "url": { "type": "text" } }
         * } }
         */
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("title", "Java设计");
        jsonMap.put("content", "咔咔咔咔咔");
        jsonMap.put("postdate", "2020-02-02");
        jsonMap.put("url", "www.baidu.com");
        IndexRequest request = new IndexRequest("index1");
        request.id("1").source(jsonMap);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.err.println(indexResponse.status()); // 返回CREATED 则添加成功
        client.close();
    }

    // 删除文档
    @Test
    public void test3() throws Exception {
        // 创建访问es服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("47.113.192.16", 9200, "http")));
        DeleteRequest request = new DeleteRequest("index1", "1");

        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
        System.err.println(deleteResponse.status()); // 返回 OK 则删除成功
        client.close();
    }

    // 更新文档
    @Test
    public void test4() throws Exception {
        // 创建访问es服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("47.113.192.16", 9200, "http")));
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("title", "大师兄师傅被妖怪抓走了");
        jsonMap.put("content", "二师兄师傅被妖怪抓走了");
        UpdateRequest request = new UpdateRequest("index1", "1").doc(jsonMap);
        UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
        System.err.println(updateResponse.status()); // 返回 OK 则修改成功
        client.close();
    }

    // upsert
    @Test
    public void test5() throws Exception {
        // 创建访问es服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("47.113.192.16", 9200, "http")));
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("title", "Java设计");
        jsonMap.put("content", "咔咔咔咔咔");
        jsonMap.put("postdate", "2020-02-02");
        jsonMap.put("url", "www.baidu.com");
        IndexRequest request1 = new IndexRequest("index1").id("2").source(jsonMap);
        jsonMap.clear();
        jsonMap.put("title", "大师兄师傅被妖怪抓走了");
        UpdateRequest request2 = new UpdateRequest("index1", "2").doc(jsonMap).upsert(request1);
        UpdateResponse updateResponse = client.update(request2, RequestOptions.DEFAULT);
        System.err.println(updateResponse.status()); // 返回 CREATED 则修改成功
        client.close();
    }

    // megt批量查询
    @Test
    public void test6() throws Exception {
        // 创建访问es服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("47.113.192.16", 9200, "http")));
        MultiGetRequest request = new MultiGetRequest();
        request.add(new MultiGetRequest.Item("index1", "1"));
        request.add(new MultiGetRequest.Item("lib10", "1"));

        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
        for (MultiGetItemResponse item : response) {
            GetResponse gr = item.getResponse();
            if (gr != null && gr.isExists()) {
                System.err.println(gr.getSourceAsString());
            }
        }
        client.close();
    }

    // bulk批量操作
    @Test
    public void test7() throws Exception {
        // 创建访问es服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("47.113.192.16", 9200, "http")));
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("index1").id("3").source(XContentType.JSON, "title", "啦啦啦", "content", "嘿嘿嘿",
                "postdate", "2020-02-02", "url", "www.baidu.com"));
//	request.add(new IndexRequest("index1").id("4")
//			.source(XContentType.JSON,
//					"title","啦啦啦",
//					"content","嘿嘿嘿",
//					"postdate","2020-02-02",
//					"url","www.baidu.com"));
//	request.add(new IndexRequest("index1").id("5")
//			.source(XContentType.JSON,
//					"title","啦啦啦",
//					"content","嘿嘿嘿",
//					"postdate","2020-02-02",
//					"url","www.baidu.com"));
//	request.add(new DeleteRequest("index1", "3"));
//	request.add(new UpdateRequest("index1", "5") 
//	        .doc(XContentType.JSON,"content", "hahaha"));
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        System.err.println(bulkResponse.status());
        if (bulkResponse.hasFailures()) {
            System.err.println("失败了");
        }
        client.close();
    }

    // TODO 第54集，查询删除
    @Test
    public void test8() throws Exception {
        // 创建访问es服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("47.113.192.16", 9200, "http")));
        DeleteByQueryRequest request = new DeleteByQueryRequest("index1");
        request.setQuery(new TermQueryBuilder("title", "Java设计"));

        BulkByScrollResponse bulkResponse = client.deleteByQuery(request, RequestOptions.DEFAULT);

        long deleted = bulkResponse.getDeleted();
        System.err.println(deleted);
        client.close();
    }

    // 查询所有
    @Test
    public void test9() throws Exception {
        // 创建访问es服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("47.113.192.16", 9200, "http")));
        SearchRequest searchRequest = new SearchRequest("index1");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.err.println(hit.getSourceAsString());

            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.err.println(key + " = " + map.get(key));
            }
        }
        client.close();
    }

    // match query
    @Test
    public void test10() throws Exception {
        // 创建访问es服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("47.113.192.16", 9200, "http")));
        SearchRequest searchRequest = new SearchRequest("index1");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("title", "大师兄师傅被妖怪抓走了"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.err.println(hit.getSourceAsString());

            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.err.println(key + " = " + map.get(key));
            }
        }
        client.close();
    }

    // multiMatchQuery
    @Test
    public void test11() throws Exception {
        // 创建访问es服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("47.113.192.16", 9200, "http")));
        SearchRequest searchRequest = new SearchRequest("index1");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("大师兄师傅被妖怪抓走了", "content", "title"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.err.println(hit.getSourceAsString());

            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.err.println(key + " = " + map.get(key));
            }
        }
        client.close();
    }

    // TODO term查询
    @Test
    public void test12() throws Exception {
        // 创建访问es服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("47.113.192.16", 9200, "http")));
        SearchRequest searchRequest = new SearchRequest("index1");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("title", "大师兄师傅被妖怪抓走了"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.err.println(hit.getSourceAsString());
        }
        client.close();
    }

    // TODO terms查询
    @Test
    public void test13() throws Exception {
        // 创建访问es服务器的客户端
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("47.113.192.16", 9200, "http")));
        SearchRequest searchRequest = new SearchRequest("index1");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termsQuery("title", "大师兄师傅被妖怪抓走了", "啦啦啦"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.err.println(hit.getSourceAsString());
        }
        client.close();
    }

}
