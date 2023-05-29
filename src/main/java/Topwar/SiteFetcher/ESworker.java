package Topwar.SiteFetcher;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.rest.RestStatus;
import org.json.simple.JSONObject;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class ESworker extends Thread{
    private static Logger log = LogManager.getLogger();
    static String queuePosts = "parsed_posts";
    private static RestHighLevelClient client;
    private Channel channel;
    private static final String indexName = "topwar";
    private static final String indexMapping = """
            {
              "mappings": {
                "properties": {
                  "datetime": {
                    "type": "date"
                  },
                  "id": {
                    "type": "keyword"
                  },
                  "text": {
                    "type": "text"
                  },
                  "title": {
                    "type": "text"
                  },
                  "url": {
                    "type": "text"
                  }
                }
              }
            }
            """;

    public ESworker(Channel channel, String hostname, int port){
        this.channel = channel;
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(hostname, port, "http")
                ));
    }
    @Override
    public void run(){
        // Создание запроса для проверки существования индекса
        GetIndexRequest request = new GetIndexRequest(indexName);

        // Выполнение запроса
        boolean response = false;
        try {
            response = client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (response) {
            queueListen();
        } else {
            try {
                createIndex();
            } catch (IOException e) {
                log.error(e);
            }
            queueListen();
        }
    }

    public static void createIndex() throws IOException {

        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
        );
        request.mapping(indexMapping, XContentType.JSON);

        try {
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            if (createIndexResponse.isAcknowledged()) {
                log.info("Index created successfully.");
            }
        } catch (InvalidIndexNameException e) {
            log.error("Invalid index name");
        } catch (ElasticsearchStatusException e) {
            if (e.status() == RestStatus.CONFLICT){
                log.error("Index with such name already Exist" + e);
            } else {
                log.error(e);
            }
        }
    }
    public static void addDocument(String documentId, String document) throws IOException {
        // Пример документа для добавления в индекс
//        String jsonString = "{\"title\":\"Заголовок документа\", \"datetime\":\"2023-05-28 10:00:00\", \"text\":\"Текст документа\", \"url\":\"http://example.com\", \"id\":\"1\"}";

        GetRequest existReq = new GetRequest(indexName, documentId);
        boolean exists = client.exists(existReq, RequestOptions.DEFAULT);

        if (!exists){
            IndexRequest request = new IndexRequest(indexName);
            request.id(documentId);
            request.source(document, XContentType.JSON);

            IndexResponse response = client.index(request, RequestOptions.DEFAULT);

            String index = response.getIndex();
            String id = response.getId();
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                log.info("Document created successfully. Index: " + index + ", ID: " + id);
            } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                log.info("Document updated successfully. Index: " + index + ", ID: " + id);
            }
        } else {
            UpdateRequest request = new UpdateRequest(indexName, document);
            UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
            log.info("Document "+ documentId + " updated");
        }
    }

    public void queueListen() {
        try {
            channel.basicConsume(queuePosts, false, "ES_ConsumerTag", new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag,
                                           Envelope envelope,
                                           AMQP.BasicProperties properties,
                                           byte[] body)
                        throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();
                    String postData = new String(body, StandardCharsets.UTF_8);
                    log.info("Начинаем обработку документа " + postData);

                    // Создание объекта Gson
                    Gson gson = new Gson();


                    // Преобразование строки в JsonObject
                    JSONObject jpostData = gson.fromJson(postData, JSONObject.class);

                    // Использование JsonObject
                    String id = jpostData.get("id").toString();
                    addDocument(id, jpostData.toJSONString());

                    channel.basicAck(deliveryTag, false);
                }
            });
        } catch (IOException e) {
            log.error(e);
        }
    }

}
