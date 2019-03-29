package gcp.cm.bigdata.adtech;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gcp.cm.bigdata.adtech.domain.Impression;
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
                ofy().save().entity(entry);
                break;
        }
    }

}
