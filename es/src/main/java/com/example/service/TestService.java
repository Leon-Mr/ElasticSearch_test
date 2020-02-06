package com.example.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.HashMap;
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

    @Test
    public void get(){
        GetRequest getRequest=new GetRequest("test","01");
        GetResponse response=null;
        try {
            response=client.get(getRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(response.getSource());
    }
}
