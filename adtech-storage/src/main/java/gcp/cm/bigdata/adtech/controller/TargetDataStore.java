package gcp.cm.bigdata.adtech.controller;

public enum TargetDataStore {
    CLOUD_DATASTORE("datastore"),
    CLOUD_BIGTABLE("bigtable");

    private String pathPrefix;

    TargetDataStore(String pathPrefix) {
        this.pathPrefix = pathPrefix;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }
}
