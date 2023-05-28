package Topwar.SiteFetcher;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;

import java.util.Collection;
import java.util.Properties;

public class Main {
    private static Logger log = LogManager.getLogger();
    private static String site = "https://topwar.ru/news/page/1/";
    private static Thread taskController, taskConsumer;

    public static void main(String[] args) {
        Properties props = System.getProperties();
//        TaskController taskController = new TaskController(site);
//        Document doc = taskController.getUrl(site);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);

        try {
            Connection conn = factory.newConnection();
            Channel channel = conn.createChannel();
            taskController = new Thread(new TaskController(channel, site));
            taskController.start();
            taskConsumer = new Thread(new TaskController(channel, null));
            taskConsumer.start();
        } catch (Exception ex) {
            log.error(ex);
            return;
        }

//        String title;
//        if (doc != null) {
//            title = doc.title();
//            log.info(title);
//            taskController.ParseNews(doc);
//        }
    }
}
