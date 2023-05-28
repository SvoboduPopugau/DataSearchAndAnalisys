package Topwar.SiteFetcher;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class ESworker extends Thread{
    private static Logger log = LogManager.getLogger();
    private static RestHighLevelClient client;
    private static final String INDEX_NAME = "topwar";
    private static final String INDEX_MAPPING = """
            {
              "mappings": {
                "properties": {
                  "title": {
                    "type": "text"
                  },
                  "time": {
                    "type": "date"
                  },
                  "text": {
                    "type": "text"
                  },
                  "link": {
                    "type": "text"
                  },
                  "id": {
                    "type": "keyword"
                  }
                }
              }
            }
            """;

    public ESworker(String hostname, int port){
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(hostname, port, "http")
                ));
    }
    @Override
    public void run(){
        System.out.println("Запустился ESworker!");
    }

    public static void createIndex() throws IOException {

        CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
        );
        request.mapping(INDEX_MAPPING, XContentType.JSON);

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
        IndexRequest request = new IndexRequest(INDEX_NAME);
        request.id(documentId);

        // Пример документа для добавления в индекс
//        String jsonString = "{\"title\":\"Заголовок документа\", \"time\":\"2023-05-28 10:00:00\", \"text\":\"Текст документа\", \"link\":\"http://example.com\", \"id\":\"1\"}";

        request.source(document, XContentType.JSON);

        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        String index = response.getIndex();
        String id = response.getId();
        if (response.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("Document created successfully. Index: " + index + ", ID: " + id);
        } else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("Document updated successfully. Index: " + index + ", ID: " + id);
        }
    }

}
