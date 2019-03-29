package gcp.cm.bigdata.adtech;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import gcp.cm.bigdata.adtech.domain.Impression;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PubsubHelper {

    private static Publisher publisher;
    private static String topicName;
    private static String projectId;

    public static void setConfig(Properties configProps) {
        projectId = configProps.getProperty("gcp.project-id");
        topicName = configProps.getProperty("gcp.topic-name");
    }

    public static Publisher getPublisher() {
        if (publisher == null) {
            try {
                ProjectTopicName topic = ProjectTopicName.of(projectId, topicName);
                publisher = Publisher.newBuilder(topic).build();
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

    public static void submitNewImpression(String entryJson) {

        ByteString data = ByteString.copyFromUtf8(entryJson);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

        ApiFuture<String> future = PubsubHelper.getPublisher().publish(pubsubMessage);

        ApiFutures.addCallback(future, new ApiFutureCallback<String>() {
            @Override
            public void onFailure(Throwable throwable) {
                System.out.println("Error publishing message : " + throwable.getMessage());
            }

            @Override
            public void onSuccess(String messageId) {
                // Once published, returns server-assigned message ids (unique within the topic)
                System.out.println("Message published successfully : " + messageId);
            }
        }, MoreExecutors.directExecutor());

    }
}
