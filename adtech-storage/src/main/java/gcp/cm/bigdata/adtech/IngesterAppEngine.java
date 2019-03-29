package gcp.cm.bigdata.adtech;

import gcp.cm.bigdata.adtech.domain.Impression;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.concurrent.atomic.AtomicInteger;

public class IngesterAppEngine implements Ingester {
    private final static int MAX_CONCURRENCY = 350;
    private final static int MAX_CONCURRENCY_DELAY = 1000;

    private AtomicInteger totAsyncRequests = new AtomicInteger();
    private AtomicInteger totRequests = new AtomicInteger();

    private WebClient client;
    private TargetDataStore target;

    @Override
    public void setTarget(TargetDataStore target) {
        this.target = target;
        client = WebClient.create(String.format(IngesterMain.baseUrl, target.getPathPrefix()));
    }

    @Override
    public TargetDataStore getTarget() {
        return target;
    }

    @Override
    public void sendEntry(Impression entry) {
        if (target == null || client == null)
            throw new IllegalStateException("Target and client must both be set");
        while (totAsyncRequests.get() > MAX_CONCURRENCY) try {
            Thread.sleep(MAX_CONCURRENCY_DELAY);
        } catch (InterruptedException e) {}
        System.out.println("Sending request: " + totRequests.get() + " of " + totAsyncRequests.get());
        WebClient.RequestHeadersSpec<?> request = client.post().body(BodyInserters.fromObject(entry));
        totAsyncRequests.incrementAndGet();
        totRequests.incrementAndGet();
        request.exchange().map(r -> {
            System.out.println("Concurrency: " + totAsyncRequests.decrementAndGet());
            System.out.println("Total Reqs: " + totRequests.get());
            return totAsyncRequests.get();
        }).subscribe();
    }

}
