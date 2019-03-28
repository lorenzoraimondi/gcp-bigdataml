package gcp.cm.bigdata.adtech;

import gcp.cm.bigdata.adtech.controller.TargetDataStore;
import gcp.cm.bigdata.adtech.domain.Impression;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.*;
import java.util.InputMismatchException;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class Ingester {
    final static int MAX_CONCURRENCY = 350;
    final static int MAX_CONCURRENCY_DELAY = 1000;

    final static Random random = new Random();

    //    final static String baseUrl = "http://localhost:8080/ingest/impression";
    final static String baseUrl = "https://qwiklabs-gcp-56c3e61809c73e4d.appspot.com/%s/impression";

    private static AtomicInteger totAsyncRequests = new AtomicInteger();
    private static AtomicInteger totRequests = new AtomicInteger();

    public static void sendOne(TargetDataStore target) {
        Impression entry = new Impression();
        entry.setImpressionId("10000174058809263569");
        entry.setClicked(1);
        entry.setHour(14103100);
        entry.setC1(1005);
        entry.setBannerPos(0);
        entry.setSiteId(235);
        entry.setSiteDomain(628);
        entry.setSiteCategory(28772);
        entry.setAppId(2386);
        entry.setAppDomain(780189);
        entry.setAppCategory(7722);
        entry.setDeviceId(99214);
        entry.setDeviceIp(6945779);
        entry.setDeviceModel(711);
        entry.setDeviceType(1);
        entry.setDeviceConnType(0);
        entry.setC14(1005);
        entry.setC15(1009);
        entry.setC16(1006);
        entry.setC17(1003);
        entry.setC18(1002);
        entry.setC19(1001);
        entry.setC20(1009);
        entry.setC21(1009);
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<String> result = restTemplate.postForEntity(String.format(baseUrl, target.getPathPrefix()), entry, String.class);
        System.out.println("result = " + result);
    }

    public static void sendFromFileToDataStore(TargetDataStore target) {
        WebClient client = WebClient.create(String.format(baseUrl, target.getPathPrefix()));
        String line;
        try {
            BufferedReader reader = new BufferedReader(new FileReader("train"));
            reader.readLine(); // skip CSV headers
            while ((line = reader.readLine()) != null) {
                Impression entry = makeImpressionEntry(line);
                while (totAsyncRequests.get() > MAX_CONCURRENCY) try {
                    Thread.sleep(MAX_CONCURRENCY_DELAY);
                } catch (InterruptedException e) {
                }
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
        } catch (InputMismatchException e) {
            System.out.println("Error in data file");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Impression makeImpressionEntry(String line) {
        Scanner scanner = new Scanner(line);
        Impression entry = new Impression();
        scanner.useDelimiter(",");
        entry.setImpressionId(scanner.next());
        entry.setClicked(scanner.nextInt());
        entry.setHour(scanner.nextInt());
        entry.setC1(scanner.nextInt());
        entry.setBannerPos(scanner.nextInt());
        entry.setSiteId(nextIntOrRandom(scanner, 100));
        entry.setSiteDomain(nextIntOrRandom(scanner, 100));
        entry.setSiteCategory(nextIntOrRandom(scanner, 50));
        entry.setAppId(nextIntOrRandom(scanner, 500));
        entry.setAppDomain(nextIntOrRandom(scanner, 300));
        entry.setAppCategory(nextIntOrRandom(scanner, 50));
        entry.setDeviceId(nextIntOrRandom(scanner, 5000));
        entry.setDeviceIp(nextIntOrRandom(scanner, 5000));
        entry.setDeviceModel(nextIntOrRandom(scanner, 300));
        entry.setDeviceType(scanner.nextInt());
        entry.setDeviceConnType(scanner.nextInt());
        entry.setC14(scanner.nextInt());
        entry.setC15(scanner.nextInt());
        entry.setC16(scanner.nextInt());
        entry.setC17(scanner.nextInt());
        entry.setC18(scanner.nextInt());
        entry.setC19(scanner.nextInt());
        entry.setC20(scanner.nextInt());
        entry.setC21(scanner.nextInt());
        return entry;
    }

    private static int nextIntOrRandom(Scanner scanner, int max) {
        try {
            return scanner.nextInt();
        } catch (InputMismatchException e) {
            scanner.next();
            return random.nextInt(max);
        }
    }

    private static int nextIntOrRandom(Scanner scanner) {
        return nextIntOrRandom(scanner, 1000);
    }

    public static void main(String[] args) {
        sendFromFileToDataStore(TargetDataStore.CLOUD_BIGTABLE);
//        sendFromFileToDataStore(TargetDataStore.CLOUD_DATASTORE);
//        sendOne(TargetDataStore.CLOUD_BIGTABLE);
    }

}
