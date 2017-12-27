package tv.freewheel.reporting.matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.sql.Statement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DimensionHiveTableStatics
{
    protected static Logger log = LoggerFactory.getLogger(DimensionHiveTableStatics.class);
    protected static Connection con = null;
    public static void main(String[] args)
    {
        if (args.length != 2) {
            System.out.println("Usage: hive.conf tableList");
            return;
        }
        Properties properties = Util.loadProperties(args[0]);
        init(properties);
        List<String> tableList = loadTables(args[1]);
        for (String table : tableList) {
            int count = statics(table);
            String logMessage = String.format("table_statics{%s:%d}", table, count);
            log.info(logMessage);
        }
    }

    private static void init(Properties properties)
    {
        String driverName =
                "org.apache.hadoop.hive.jdbc.HiveDriver";
        try {
            Class.forName(driverName);
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        String host = properties.getProperty("host");
        String port = properties.getProperty("port");
        String db = properties.getProperty("db");
        String user = properties.getProperty("user");
        String passwd = properties.getProperty("passwd");
        String url = String.format("jdbc:hive://%s:%s/%s", host, port, db);
        try {
            con = DriverManager.getConnection(url, user, passwd);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static List<String> loadTables(String tablePath)
    {
        List<String> paths = new ArrayList<>();
        InputStream inputStream = Util.loadInputStream(tablePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String tableName;
        try {
            while ((tableName = bufferedReader.readLine()) != null) {
                paths.add(tableName);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return paths;
    }

    public static int statics(String tableName)
    {
        String sql = "select count(*) from " + tableName;
        int count = 0;
        Statement stmt;
        try {
            stmt = con.createStatement();
            ResultSet res = stmt.executeQuery(sql);
            if (res.next()) {
                count = res.getInt(0);
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }
}
