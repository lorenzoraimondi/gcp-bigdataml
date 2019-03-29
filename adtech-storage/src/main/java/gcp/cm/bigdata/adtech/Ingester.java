package gcp.cm.bigdata.adtech;

import gcp.cm.bigdata.adtech.domain.Impression;

public interface Ingester {
    void setTarget(TargetDataStore target);
    TargetDataStore getTarget();
    void sendEntry(Impression entry);
}
