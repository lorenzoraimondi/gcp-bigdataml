package gcp.cm.bigdata.adtech;

public enum IngesterMode {
    APP_ENGINE {
        @Override
        Ingester getIngester() {
            return new IngesterAppEngine();
        }
    }, DIRECT {
        @Override
        Ingester getIngester() {
            return new IngesterDirect();
        }
    };

    abstract Ingester getIngester();
}
