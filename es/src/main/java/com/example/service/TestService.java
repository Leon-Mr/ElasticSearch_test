package com.example.service;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
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


    public void put(){
        System.out.println(111);
        IndexRequest request=new IndexRequest("test");
        request.id("01");
        request.source(JSON.toJSON("无语"), XContentType.JSON);
        try {
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void get(){
        GetRequest getRequest=new GetRequest("test","_doc","01");
        GetResponse response=null;

        try {
            response=client.get(getRequest,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(response.getSource());
    }
}
