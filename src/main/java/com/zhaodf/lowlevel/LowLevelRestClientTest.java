package com.zhaodf.lowlevel;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 类：LowLevelRestClientTest
 *
 * @author zhaodf
 * @date 2020/10/27
 */
public class LowLevelRestClientTest {
    private static final String CLUSTER_HOSTNAME_PORT = "127.0.0.1:9200,127.0.0.1:8200,127.0.0.1:8000";

    //公共的：可以为所有请求设置公共的请求选项
    private static final RequestOptions COMMON_OPTIONS;
    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader("cats", "knock things off of other things");
        builder.setHttpAsyncResponseConsumerFactory(
                new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(30 * 1024 * 1024 * 1024));
        COMMON_OPTIONS = builder.build();
    }

    //1、初始化连接
    public static RestClient getClient() {
        String hostnamesPort[] = CLUSTER_HOSTNAME_PORT.split(",");

        String host;
        int port;
        String[] temp;
        //可以通过相应的RestClientBuilder类（通过RestClient＃builder（HttpHost ...）静态方法创建）来构建RestClient实例。
        // 唯一需要的参数是客户端将与之通信的一个或多个主机，以HttpHost的实例形式提供，如下所示：
        RestClientBuilder restClientBuilder = null;

        if (null != hostnamesPort) {
            for (String hostPort : hostnamesPort) {
                temp = hostPort.split(":");
                host = temp[0].trim();
                port = Integer.parseInt(temp[1].trim());
                restClientBuilder = RestClient.builder(new HttpHost(host, port, "http"));
            }
        }

        return restClientBuilder.build();
    }

    //2、发送请求
    // 一旦创建RestClient，就可以通过调用performRequest或performRequestAsync发送请求。 performRequest是同步的，将阻止调用线程并在请求成功时返回响应，如果请求失败则抛出异常。
    // performRequestAsync是异步的，接受请求成功时以Response调用的ResponseListener参数，如果失败则以Exception调用。
    //①You can add request parameters to the request object
    public static Response performRequest(RestClient restClient) throws IOException {
        Request request = new Request("GET", "/schools/school/1");
        request.addParameter("pretty", "true");
        Response response = restClient.performRequest(request);
        return response;
    }

    //②You can set the body of the request to any HttpEntity
    public static Response performRequest(RestClient restClient, NStringEntity nStringEntity) throws IOException {
        Request request = new Request("GET", "/schools/school/_search/");
        request.setEntity(nStringEntity);
        request.addParameter("pretty", "true");
        Response response = restClient.performRequest(request);
        return response;
    }

    //③You can also set it to a String which will default to a ContentType of application/json.
    public static Response performRequest(RestClient restClient, String json) throws IOException {
        Request request = new Request("POST", "/schools/school/");
        request.setJsonEntity(json);
        request.addParameter("pretty", "true");
        Response response = restClient.performRequest(request);
        return response;
    }

    //④ execute many actions in parallel，In a real world scenario you’d probably want to use the _bulk API instead, but the example is illustative.
    public static void performAsynchronousRequest(RestClient restClient,NStringEntity[] documents) throws IOException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(documents.length);
        for (int i = 0; i < documents.length; i++) {
            Request request = new Request("PUT", "/posts/doc/" + i);
            //let's assume that the documents are stored in an HttpEntity array
            request.setEntity(documents[i]);
            restClient.performRequestAsync(
                    request,
                    new ResponseListener() {
                        @Override
                        public void onSuccess(Response response) {
                            latch.countDown();
                        }

                        @Override
                        public void onFailure(Exception exception) {
                            latch.countDown();
                        }
                    }
            );
        }
        latch.await();
    }

    //3、处理请求
    public static void readResponse(RestClient restClient,Response response) throws IOException {
        //Information about the performed request
        RequestLine requestLine = response.getRequestLine();
        System.out.println(requestLine);
        //The host that returned the response
        HttpHost host = response.getHost();
        System.out.println(host.getAddress());
        //The response status line, from which you can for instance retrieve the status code
        int statusCode = response.getStatusLine().getStatusCode();
        System.out.println(statusCode);
        //The response headers, which can also be retrieved by name though getHeader(String)
        Header[] headers = response.getHeaders();
        System.out.println(headers.toString());
        //The response body enclosed in an org.apache.http.HttpEntity object
        String responseBody = EntityUtils.toString(response.getEntity());
        System.out.println(responseBody);
    }

    public static void main(String[] args) {
        RestClient restClient = LowLevelRestClientTest.getClient();
        try {
//            ------------------一-----------------------------
//             Response response = performRequest(restClient);
//            ------------------二-----------------------------
//            NStringEntity nStringEntity = new NStringEntity("{\n" +
//                    "  \"query\": {\n" +
//                    "    \"match_all\": {}\n" +
//                    "  }\n" +
//                    "}", ContentType.APPLICATION_JSON);
//            Response response = performRequest(restClient,nStringEntity);
            String json = "{\n" +
                    "  \"city\": " +
                    "    \"zhaodf\" " +
                    "  }" ;
            Response response = performRequest(restClient,json);
            readResponse(restClient,response);
            restClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
