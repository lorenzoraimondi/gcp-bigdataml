package gcp.cm.bigdata.adtech;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.ProjectTopicName;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class PubsubHelper {

    private static Publisher publisher;

    public static Publisher getPublisher() {
        if (publisher == null) {
            try {
                ProjectTopicName topicName = ProjectTopicName.of("qwiklabs-gcp-56c3e61809c73e4d", "ad-impressions");
                publisher = Publisher.newBuilder(topicName).build();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return publisher;
    }

    public static void shutdownPublisher() {
        if (publisher != null) {
            try {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
