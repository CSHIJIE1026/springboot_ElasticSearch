package com.usian.test;

import com.usian.ElasticsearchApp;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {ElasticsearchApp.class})
public class IndexReaderTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private SearchRequest searchRequest;
    private SearchResponse searchResponse;

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

    @Before
    public void initSearchRequest(){
        //搜索请求对象
        searchRequest = new SearchRequest("java1906");
        searchRequest.types("course");
    }

    //DSL搜索
    @Test
    public void testSearchAll() throws IOException {
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // 设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }
    //DSL搜索分页查询
    @Test
    public void testSearchPage() throws IOException {
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.from (1);
        searchSourceBuilder.size (2);
        searchSourceBuilder.sort ("price", SortOrder.DESC);
        // 设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //match查询
    @Test
    public void testMatchQuery() throws IOException {
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery ("name","spring开发").operator (Operator.AND));
        // 设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //multi_match查询
    @Test
    public void testMultiMatchQuery() throws IOException {
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery ("开发","name","description"));
        // 设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //bool查询
    @Test
    public void testBooleanQuery() throws IOException {
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //bool
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery ();
        boolQueryBuilder.must (QueryBuilders.matchQuery ("name","开发"));
        boolQueryBuilder.must (QueryBuilders.rangeQuery ("price").gte ("1").lte ("100"));

        searchSourceBuilder.query(boolQueryBuilder);
        // 设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //filter查询
    @Test
    public void testFilterQuery() throws IOException {
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //bool
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery ();
        boolQueryBuilder.must (QueryBuilders.matchQuery ("name","开发"));
        boolQueryBuilder.filter (QueryBuilders.rangeQuery ("price").gte ("1").lte ("100"));

        searchSourceBuilder.query(boolQueryBuilder);
        // 设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //highlight查询
    @Test
    public void testHighLightQuery() throws IOException {
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query (QueryBuilders.matchQuery ("name","spring"));

        //设置高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder ();
        highlightBuilder.preTags ("<red>");
        highlightBuilder.postTags ("</red>");
        highlightBuilder.fields ().add (new HighlightBuilder.Field ("name"));
        searchSourceBuilder.highlighter (highlightBuilder);

        // 设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    @After
    public void displayDoc(){
        //搜索匹配结果
        SearchHits hits = searchResponse.getHits();

        //搜索总记录数
        long totalHits = hits.totalHits;
        System.out.println("共搜索到" + totalHits + "条文档");

        //匹配的文档
        SearchHit[] hitsHits = hits.getHits();

        for (SearchHit hitsHit : hitsHits) {
            System.out.println(hitsHit.getSourceAsString());

            Map<String, HighlightField> highlightFields = hitsHit.getHighlightFields ();
            if (highlightFields != null){
                HighlightField highlightField = highlightFields.get ("name");
                Text[] fragments = highlightField.getFragments ();
                System.out.println ("高亮字段：" + fragments[0].toString());
            }

        }
    }
}
