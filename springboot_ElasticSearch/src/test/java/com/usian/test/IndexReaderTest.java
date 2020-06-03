package com.usian.test;

import com.usian.ElasticsearchApp;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {ElasticsearchApp.class})
public class IndexReaderTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    //查询文档
    @Test
    public void getDoc() throws IOException {
        GetRequest getRequest = new GetRequest("java1906", "course", "1");
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        boolean exists = getResponse.isExists();
        System.out.println(exists);
        String sourceAsString = getResponse.getSourceAsString();
        System.out.println(sourceAsString);
    }

    //DSL搜索
    @Test
    public void testSearchAll() throws IOException {
        //搜索请求对象
        SearchRequest searchRequest = new SearchRequest("java1906");
        searchRequest.types("course");

        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        // 设置搜索源
        searchRequest.source(searchSourceBuilder);

        //执行搜索
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //搜索匹配结果
        SearchHits hits = searchResponse.getHits();

        //搜索总记录数
        long totalHits = hits.totalHits;
        System.out.println("共搜索到" + totalHits + "条文档");

        //匹配的文档
        SearchHit[] hitsHits = hits.getHits();

        for (SearchHit hitsHit : hitsHits) {
            System.out.println(hitsHit.getSourceAsString());
        }
    }
}
