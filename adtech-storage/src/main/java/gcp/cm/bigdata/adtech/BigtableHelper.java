package gcp.cm.bigdata.adtech;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

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

}
