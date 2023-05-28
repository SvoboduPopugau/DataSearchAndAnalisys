package Topwar.SiteFetcher;

import com.rabbitmq.client.*;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.json.simple.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import com.rabbitmq.client.Channel;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;


public class TaskController extends Thread{
    private static Logger log = LogManager.getLogger();
    static String exchangeName = "";
    static String queueUrl = "start_urls";
    static String exchangeName_1 = "";
    static String queuePosts = "parsed_posts";
    private CloseableHttpClient client = null;
    private HttpClientBuilder builder;
    private String server = "https://topwar.ru/news/";
    private int retryDelay = 5 * 1000;
    private int retryCount = 2;
    private int metadataTimeout = 30 * 1000;
    private Channel channel;
    private static final String INDEX_NAME = "topwar";
    private RestHighLevelClient esclient;

    public TaskController(Channel channel, String _server) {
        this.channel = channel;
        CookieStore httpCookiesStore = new BasicCookieStore();
        builder = HttpClientBuilder.create().setDefaultCookieStore(httpCookiesStore);
        client = builder.build();
        this.server = _server;
        esclient = new RestHighLevelClient(
                RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        ));
    }
    @Override
    public void run(){
        if (server != null) {
            ParseNews(getUrl(server));
        } else {
            getPage();
        }
    }

    public void publishToMQ (String message, String exName, String queueName) {
        byte [] messageBodyBytes = message.getBytes();
        try {
            channel.basicPublish(
                    exName,
                    queueName,
                    false,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    messageBodyBytes
            );
        } catch (Exception ex) {
            log.error(ex);
        }
    }

//    Скачивает html-страницу и парсит в jsoup.document
    public Document getUrl(String url) {
        int code = 0;
        boolean bStop = false;
        Document doc = null;
        for (int iTry = 0; iTry < retryCount && !bStop; iTry++) {
            log.info("getting page from url " + url);
//            client = builder.build()
//            client = HttpClientBuilder.create().build()
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(metadataTimeout)
                    .setConnectTimeout(metadataTimeout)
                    .setConnectionRequestTimeout(metadataTimeout)
                    .setExpectContinueEnabled(true)
                    .build();
            HttpGet request = new HttpGet(url);
            request.setConfig(requestConfig);
            CloseableHttpResponse response = null;
            try {
                response = client.execute(request);
                code = response.getStatusLine().getStatusCode();
                if (code == 404) {
                    log.warn("error get url " + url + " code " + code);
                    bStop = true;
                }else if (code == 403) {
                    log.warn("error get url " + url + " code " + code);
                    bStop = true;
                } else if (code == 200) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        try {
                            if (server != null){
                                doc = Jsoup.parse(entity.getContent(), "UTF-8", server);
                            } else {
                                doc = Jsoup.parse(entity.getContent(), "UTF-8", url);
                            }
                            break;
                        } catch (IOException e) {
                            log.error(e);
                        }
                    }
                    bStop = true;
                } else {
                    log.warn("error get url " + url + " code " + code);
                    response.close();
                    response = null;
                    client.close();
                    CookieStore httpCookieStore = new BasicCookieStore();
                    builder.setDefaultCookieStore(httpCookieStore);
                    client = builder.build();
                    int delay = retryDelay * 1000 * (iTry + 1);
                    log.info("wait " + delay / 1000 + " s...");
                    try {
                        Thread.sleep(delay);
                        continue;
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            } catch (IOException e){
                log.error(e);
            }
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    log.error(e);
                }
            }
        }
        return doc;
    }

//    Собираем все ссылки с начальной страницы и отправляем в очередь
    public void ParseNews(Document doc) {
        try {
            Elements news = doc.select("#dle-content > article > div.post-cont > h2 > a");
            for (Element element : news) {
                try {
                    String link = element.attr("href");
                    log.info(element.text());

//                    TODO: Рассчитываем хеш-код от text и href и проверяем в БД
                    publishToMQ(link, exchangeName, queueUrl);
//                    JSONObject newsData = getPage(link);
//                    if (newsData != null){
//                        log.info(newsData);
//                    } else {
//                        log.warn("Страница " + link + " пуста");
//                    }
                } catch (Exception e) {
                    log.error(e);
                }
            }
        } catch (Exception ex) {
            log.error(ex);
        }
    }

//    Собираем Текст статьи, ссылку, заголовок и время публикации
    public void getPage() {
        try {
            channel.basicConsume(queueUrl, false, "ConsumerTag", new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag,
                                           Envelope envelope,
                                           AMQP.BasicProperties properties,
                                           byte[] body)
                        throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();
                    String link = new String(body, StandardCharsets.UTF_8);
                    log.info("Начинаем обработку странички " + link);

                    Document ndoc = getUrl(link);
//                    Document ndoc = Jsoup.connect(link).get();
                    JSONObject file = new JSONObject();
                    String title = null;
                    String datetime = null;
                    String text = null;
                    if (ndoc != null) {
                        title = Objects.requireNonNull(ndoc.selectFirst("#full-story > article > h1")).text();
                        datetime = Objects.requireNonNull(ndoc.selectFirst("#full-story > article > div.meta.fs-0875.c-muted.fw-b > time")).attr("datetime");
                        Elements textelements = ndoc.getElementsByClass("pfull-cont text");
                        text = textelements.text();

                        file.put("url", link);
                        file.put("datetime", datetime);
                        file.put("title", title);
                        file.put("text", text);
                    }
                    log.info("Инфорамция о статье " + file.toString());
//                    TODO: Добавить в очередь где будут результаты парсинга статьи
                    publishToMQ(file.toString(), exchangeName_1, queuePosts);
                    channel.basicAck(deliveryTag, false);
                }
            });
        } catch (IOException e) {
            log.error(e);
        }


    }
}
