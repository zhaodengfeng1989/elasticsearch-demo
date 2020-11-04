package com.zhaodf.highlevel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.Date;

/**
 * 类：LowLevelRestClientTest
 *
 * @author zhaodf
 * @date 2020/10/27
 */
public class HighLevelRestClientTest {
    //1、初始化客户端连接：高级客户端基于低级客户端进行客户端连接创建
    public static RestHighLevelClient getClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 8200, "http"),
                        new HttpHost("localhost", 8000, "http")));
        return client;
    }

    //2、索引创建
    // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.5/java-rest-high-create-index.html
    public static void createIndex(RestHighLevelClient client) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("twitter");
        //Each index created can have specific settings associated with it.
        request.settings(Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)
        );
        //An index may be created with mappings for its document types
        // request.mapping("_doc", "message", "type=text");
        request.mapping("_doc",
                "{\n" +
                        "  \"_doc\": {\n" +
                        "    \"properties\": {\n" +
                        "      \"message\": {\n" +
                        "        \"type\": \"text\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }\n" +
                        "}",
                XContentType.JSON);

        request.alias(new Alias("twitter_alias").filter(QueryBuilders.termQuery("user", "kimchy")));

        //等价于
        // request.source("{\n" +
        //        "    \"settings\" : {\n" +
        //        "        \"number_of_shards\" : 1,\n" +
        //        "        \"number_of_replicas\" : 0\n" +
        //        "    },\n" +
        //        "    \"mappings\" : {\n" +
        //        "        \"_doc\" : {\n" +
        //        "            \"properties\" : {\n" +
        //        "                \"message\" : { \"type\" : \"text\" }\n" +
        //        "            }\n" +
        //        "        }\n" +
        //        "    },\n" +
        //        "    \"aliases\" : {\n" +
        //        "        \"twitter_alias\" : {}\n" +
        //        "    }\n" +
        //        "}", XContentType.JSON);
        //The following arguments can optionally be provided
        request.timeout(TimeValue.timeValueMinutes(2));
        request.timeout("2m");

        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        request.masterNodeTimeout("1m");
        //同步方法创建索引:create-index-sync
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);

        boolean acknowledged = createIndexResponse.isAcknowledged();
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
        System.out.println("acknowledged="+acknowledged+",shardsAcknowledged="+shardsAcknowledged);
    }

    //3、创建文档
    // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.5/java-rest-high-document-index.html
    public static void createDocument(RestHighLevelClient client) throws IOException {
        IndexRequest request = new IndexRequest("posts", "doc", "1")
                .source("user", "kimchy",
                        "postDate", new Date(),
                        "message", "trying out Elasticsearch");
        //等价于
        // Map<String, Object> jsonMap = new HashMap<>();
        //jsonMap.put("user", "kimchy");
        //jsonMap.put("postDate", new Date());
        //jsonMap.put("message", "trying out Elasticsearch");
        //IndexRequest indexRequest = new IndexRequest("posts", "doc", "1")
        //        .source(jsonMap);
        //The following arguments can optionally be provided:
        request.routing("routing");
        //request.parent("parent");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");
        request.opType(DocWriteRequest.OpType.CREATE);
        request.opType("create");
        //request.setPipeline("pipeline");
        //When executing a IndexRequest in the following manner, the client waits for the IndexResponse to be returned before continuing with code execution:
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        String index = indexResponse.getIndex();
        String type = indexResponse.getType();
        String id = indexResponse.getId();
        long version = indexResponse.getVersion();
        System.out.println("index："+index+",type="+type+",id="+id+",version="+version);
    }

    public static void main(String[] args) {
        try {
            HighLevelRestClientTest.createDocument(HighLevelRestClientTest.getClient());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
