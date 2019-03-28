package gcp.cm.bigdata.adtech.controller;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import gcp.cm.bigdata.adtech.PubsubHelper;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("pubsub")
@CrossOrigin
public class PubsubIngestController {

    @RequestMapping(path = "impression", method = RequestMethod.POST)
    public void ingest(@RequestBody String entryJson) {

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