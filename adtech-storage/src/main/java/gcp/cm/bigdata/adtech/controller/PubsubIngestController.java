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
        PubsubHelper.submitNewImpression(entryJson);
    }

}