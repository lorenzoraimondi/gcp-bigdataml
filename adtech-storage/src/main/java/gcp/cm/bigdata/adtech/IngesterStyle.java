package gcp.cm.bigdata.adtech;

import java.util.function.Consumer;

public enum IngesterStyle {
    SINGLE {
        @Override
        Consumer<Ingester> getFunction() {
            return IngesterMain::sendFromFileToDataStore;
        }
    }, FROM_FILE {
        @Override
        Consumer<Ingester> getFunction() {
            return IngesterMain::sendFromFileToDataStore;
        }
    };

    abstract Consumer<Ingester> getFunction();
}
