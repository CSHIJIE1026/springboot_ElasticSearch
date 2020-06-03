package com.usian.test;

import com.usian.ElasticsearchApp;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {ElasticsearchApp.class})
public class IndexWriterTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    //创建索引库
    @Test
    public void testCreateIndex() throws IOException {
        //创建"创建索引请求"对象,并设置索引名称
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("java1906");
        //设置索引参数
        createIndexRequest.settings(Settings.builder().
                put("number_of_shards",1).
                put("number_of_replicas",0));
        createIndexRequest.mapping("course","{\n" +
                "  \"_source\": {\n" +
                "    \"excludes\":[\"description\"]\n" +
                "  }, \n" +
                "  \"properties\": {\n" +
                "    \"name\":{\n" +
                "      \"type\": \"text\",\n" +
                "      \"analyzer\": \"ik_max_word\",\n" +
                "      \"search_analyzer\": \"ik_smart\"\n" +
                "    },\n" +
                "    \"description\":{\n" +
                "      \"type\": \"text\",\n" +
                "      \"analyzer\": \"ik_max_word\",\n" +
                "      \"search_analyzer\": \"ik_smart\"\n" +
                "    },\n" +
                "    \"studymodel\":{\n" +
                "      \"type\": \"keyword\"\n" +
                "    },\n" +
                "    \"pic\":{\n" +
                "      \"type\": \"text\",\n" +
                "      \"index\": false\n" +
                "    },\n" +
                "    \"timestamp\":{\n" +
                "      \"type\": \"date\",\n" +
                "      \"format\":\"yyyy-MM-dd HH:mm:ss || yyyy-MM-dd\"\n" +
                "    },\n" +
                "    \"price\":{\n" +
                "      \"type\": \"float\"\n" +
                "    }\n" +
                "  }\n" +
                "}", XContentType.JSON);
        //创建索引操作客户端
        IndicesClient indices = restHighLevelClient.indices();
        //创建响应对象
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest);
        //得到响应结果
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }


    //删除索引库
    @Test
    public void testDeleteIndex() throws IOException {
        //创建“删除索引请求”对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("java1906");
        //创建索引操作客户端
        IndicesClient indices = restHighLevelClient.indices();
        //创建响应对象
        DeleteIndexResponse deleteIndexResponse = indices.delete(deleteIndexRequest, RequestOptions.DEFAULT);
        //得到响应结果
        boolean acknowledged = deleteIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }


    //添加文档
    @Test
    public void testAddDocument() throws IOException {
        //创建“索引请求”对象：索引当动词
        IndexRequest indexRequest = new IndexRequest("java1906", "course", "1");
        indexRequest.source("{\n" +
                "  \"name\": \"spring cloud实战\",\n" +
                "  \"description\": \"本课程主要从四个章节进行讲解： 1.微服务架构入门 2.spring cloud 基础入门 3.实战Spring Boot 4.注册中心eureka。\",\n" +
                "  \"studymodel\": \"201001\",\n" +
                "  \"price\": 5.7\n" +
                "}",XContentType.JSON);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.toString());
    }


    //批量添加文档
    @Test
    public void testBulkAddDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("java1906","course").source("{\"name\":\"php实战\",\"description\":\"php谁都不服\",\"studymodel\":\"201001\",\"price\":\"5.8\"}",
                XContentType.JSON));
        bulkRequest.add(new IndexRequest("java1906","course").source("{\"name\":\"net实战\",\"description\":\"net从入门到放弃\",\"studymodel\":\"201001\",\"price\":\"7.6\"}",
                XContentType.JSON));
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.hasFailures());
    }

    //更新文档
    @Test
    public void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("java1906", "course", "1");
        updateRequest.doc("{\n" +
                " \"price\":6.66\n" +
                "}",XContentType.JSON);
        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        DocWriteResponse.Result result = updateResponse.getResult();
        System.out.println(result);
    }

    //删除文档
    @Test
    public void testDelDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("java1906", "course", "1");
        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.getResult());
    }

}
