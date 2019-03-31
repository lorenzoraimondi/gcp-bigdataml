package gcp.cm.bigdata.adtech;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.googlecode.objectify.Objectify;
import com.googlecode.objectify.ObjectifyService;
import gcp.cm.bigdata.adtech.domain.Impression;

import java.io.Closeable;
import java.io.IOException;

import static com.googlecode.objectify.ObjectifyService.ofy;

public class IngesterDirect implements Ingester {
    private TargetDataStore target;
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public void setTarget(TargetDataStore target) {
        this.target = target;
    }

    @Override
    public TargetDataStore getTarget() {
        return target;
    }

    @Override
    public void sendEntry(Impression entry) {
        switch (target) {
            case CLOUD_PUBSUB:
                try {
                    String json = mapper.writeValueAsString(entry);
                    PubsubHelper.submitNewImpression(json);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                break;
            case CLOUD_BIGTABLE:
                BigtableHelper.insertNewImpression(entry);
                break;
            case CLOUD_DATASTORE:
                try (Closeable c = ObjectifyService.begin()) {
                    ofy().save().entity(entry);
                } catch (IOException e) {
                    System.out.println("Data store error");
                    e.printStackTrace();
                }
                break;
        }
    }

}
