package gcp.cm.bigdata.adtech;

import gcp.cm.bigdata.adtech.domain.Impression;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class BigtableHelper {

    private static Connection connection;

    public static Connection getConnection() {
        if (connection == null) {
            try {
                connection = ConnectionFactory.createConnection();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void insertNewImpression(Impression entry) {
        byte[] CF1 = Bytes.toBytes("I");  // column family
        byte[] CF2 = Bytes.toBytes("A");  // column family
        byte[] CF3 = Bytes.toBytes("D");  // column family
        byte[] CF4 = Bytes.toBytes("S");  // column family
        byte[] CF5 = Bytes.toBytes("C");  // column family
        Table table = null;
        try {
            table = BigtableHelper.getConnection().getTable(TableName.valueOf("impressions"));
            Put p = new Put(Bytes.toBytes(String.format("%s#%s#%d#%d", entry.getSiteCategory(), entry.getAppCategory(), entry.getDeviceType(), entry.getHour())));
            p.addColumn(CF1, Bytes.toBytes("ID"), Bytes.toBytes(entry.getImpressionId()));
            p.addColumn(CF1, Bytes.toBytes("CLICK"), Bytes.toBytes(entry.getClicked()));
            p.addColumn(CF1, Bytes.toBytes("HOUR"), Bytes.toBytes(entry.getHour()));
            p.addColumn(CF4, Bytes.toBytes("SITE"), Bytes.toBytes(entry.getSiteId()));
            p.addColumn(CF4, Bytes.toBytes("SITED"), Bytes.toBytes(entry.getSiteDomain()));
            p.addColumn(CF4, Bytes.toBytes("SITEC"), Bytes.toBytes(entry.getSiteCategory()));
            p.addColumn(CF2, Bytes.toBytes("APP"), Bytes.toBytes(entry.getAppId()));
            p.addColumn(CF2, Bytes.toBytes("APPD"), Bytes.toBytes(entry.getAppDomain()));
            p.addColumn(CF2, Bytes.toBytes("APPC"), Bytes.toBytes(entry.getAppCategory()));
            p.addColumn(CF3, Bytes.toBytes("DEV"), Bytes.toBytes(entry.getDeviceId()));
            p.addColumn(CF3, Bytes.toBytes("DEVIP"), Bytes.toBytes(entry.getDeviceIp()));
            p.addColumn(CF3, Bytes.toBytes("DEVMD"), Bytes.toBytes(entry.getDeviceModel()));
            p.addColumn(CF3, Bytes.toBytes("DEVT"), Bytes.toBytes(entry.getDeviceType()));
            p.addColumn(CF3, Bytes.toBytes("DEVCT"), Bytes.toBytes(entry.getDeviceConnType()));
            p.addColumn(CF5, Bytes.toBytes("C1"), Bytes.toBytes(entry.getC1()));
            p.addColumn(CF5, Bytes.toBytes("C14"), Bytes.toBytes(entry.getC14()));
            p.addColumn(CF5, Bytes.toBytes("C15"), Bytes.toBytes(entry.getC15()));
            p.addColumn(CF5, Bytes.toBytes("C16"), Bytes.toBytes(entry.getC16()));
            p.addColumn(CF5, Bytes.toBytes("C17"), Bytes.toBytes(entry.getC17()));
            p.addColumn(CF5, Bytes.toBytes("C18"), Bytes.toBytes(entry.getC18()));
            p.addColumn(CF5, Bytes.toBytes("C19"), Bytes.toBytes(entry.getC19()));
            p.addColumn(CF5, Bytes.toBytes("C20"), Bytes.toBytes(entry.getC20()));
            p.addColumn(CF5, Bytes.toBytes("C21"), Bytes.toBytes(entry.getC21()));
            table.put(p);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
