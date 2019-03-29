package gcp.cm.bigdata.adtech;

public enum TargetDataStore {
    CLOUD_DATASTORE("datastore"),
    CLOUD_BIGTABLE("bigtable"),
    CLOUD_PUBSUB("pubsub");

    private String pathPrefix;

    TargetDataStore(String pathPrefix) {
        this.pathPrefix = pathPrefix;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }
}
