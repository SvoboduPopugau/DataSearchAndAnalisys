package Topwar.SiteFetcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;

public class Main {
    private static Logger log = LogManager.getLogger();
    private static String site = "https://topwar.ru/news/";

    public static void main(String[] args) {
        TaskController taskController = new TaskController(site);
        Document doc = taskController.getUrl(site);
        String title;
        if (doc != null) {
            title = doc.title();
            log.info(title);
            taskController.ParseNews(doc);
        }
    }
}
