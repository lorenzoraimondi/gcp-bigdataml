package gcp.cm.bigdata.adtech;

import gcp.cm.bigdata.adtech.domain.Impression;

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
    static TargetDataStore target;
    static IngesterMode mode;
    static IngesterStyle style;

    public static void setConfig(Properties configProps) {
        projectId = configProps.getProperty("gcp.project-id");
        baseUrl = "https://" + projectId + ".appspot.com/%s/impression";
        fileToRead = configProps.getProperty("ingest.filename");
        target = TargetDataStore.valueOf(configProps.getProperty("ingest.target"));
        mode = IngesterMode.valueOf(configProps.getProperty("ingest.mode"));
        style = IngesterStyle.valueOf(configProps.getProperty("ingest.style"));
    }

    public static void sendOne(Ingester ingester) {
        Impression entry = new Impression();
        entry.setImpressionId("10000174058809263569");
        entry.setClicked(1);
        entry.setHour(14103100);
        entry.setC1(1005);
        entry.setBannerPos(0);
        entry.setSiteId("235");
        entry.setSiteDomain("628");
        entry.setSiteCategory("28772");
        entry.setAppId("2386");
        entry.setAppDomain("780189");
        entry.setAppCategory("7722");
        entry.setDeviceId("99214");
        entry.setDeviceIp("6945779");
        entry.setDeviceModel("711");
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
        String line = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileToRead));
            reader.readLine(); // skip CSV headers
            while ((line = reader.readLine()) != null) {
                Impression entry = makeImpressionEntry(line);
                ingester.sendEntry(entry);
            }
        } catch (InputMismatchException e) {
            System.out.println("Error in data file");
            System.out.println("line:" + line);
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
        entry.setSiteId(scanner.next());
        entry.setSiteDomain(scanner.next());
        entry.setSiteCategory(scanner.next());
        entry.setAppId(scanner.next());
        entry.setAppDomain(scanner.next());
        entry.setAppCategory(scanner.next());
        entry.setDeviceId(scanner.next());
        entry.setDeviceIp(scanner.next());
        entry.setDeviceModel(scanner.next());
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
        setConfig(props);
        Ingester ingester = mode.getIngester();
        ingester.setTarget(target);
        style.getFunction().accept(ingester);
    }

}
