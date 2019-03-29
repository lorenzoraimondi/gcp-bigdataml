package gcp.cm.bigdata.adtech;

import gcp.cm.bigdata.adtech.domain.Impression;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.util.InputMismatchException;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

public class IngesterMain {
    final static Random random = new Random();

    static String projectId;
    static String baseUrl;
    static String fileToRead;

    public static void setConfig(Properties configProps) {
        projectId = configProps.getProperty("gcp.project-id");
        baseUrl = "https://" + projectId + ".appspot.com/%s/impression";
        fileToRead = configProps.getProperty("ingest.filename");
    }

    public static void sendOne(Ingester ingester) {
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
        ingester.sendEntry(entry);
    }

    public static void sendFromFileToDataStore(Ingester ingester) {
        String line;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileToRead));
            reader.readLine(); // skip CSV headers
            while ((line = reader.readLine()) != null) {
                Impression entry = makeImpressionEntry(line);
                ingester.sendEntry(entry);
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

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.load(HttpApiMain.class.getResourceAsStream("/gcp.properties"));
        TargetDataStore target = TargetDataStore.valueOf(props.getProperty("ingest.target"));
        IngesterMode mode = IngesterMode.valueOf(props.getProperty("ingest.mode"));
        IngesterStyle style = IngesterStyle.valueOf(props.getProperty("ingest.style"));
        Ingester ingester = mode.getIngester();
        ingester.setTarget(target);
        style.getFunction().accept(ingester);
    }

}
