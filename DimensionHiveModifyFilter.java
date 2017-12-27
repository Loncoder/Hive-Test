package tv.freewheel.reporting.matcher;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

public class DimensionHiveModifyFilter
{
    protected static Logger log = LoggerFactory.getLogger(DimensionHiveModifyFilter.class);
    private static Configuration conf = new Configuration();
    private static JsonParser jsonParser = new JsonParser();
    private static final String hdfsPrefix = "hdfs://";

    public static void loadSchedulerOutPut(String schedulerOutPath, Set<String> schedulerOutSet)
    {
        InputStream inputStream = Util.loadInputStream(schedulerOutPath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String tableName;
        try {
            while ((tableName = bufferedReader.readLine()) != null) {
                schedulerOutSet.add(tableName);
            }
            bufferedReader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getModifyTime(Path path, FileSystem fs, Map<String, Map<String, Long>> fileModifyTimesMap)
    {
        FileStatus[] fileStatus = new FileStatus[0];
        try {
            fileStatus = fs.listStatus(path);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < fileStatus.length; i++) {
            if (fileStatus[i].isDir()) {
                Path p = new Path(fileStatus[i].getPath().toString());
                getModifyTime(p, fs, fileModifyTimesMap);
            }
            else {
                String pathStr = fileStatus[i].getPath().toString();
                if (pathStr.startsWith(hdfsPrefix)) {
                    pathStr = pathStr.substring(pathStr.indexOf("/", hdfsPrefix.length() + 1));
                }
                String tableName = pathStr.substring(pathStr.lastIndexOf("/") + 1);
                if (!fileModifyTimesMap.containsKey(tableName)) {
                    fileModifyTimesMap.put(tableName, new HashMap<String, Long>());
                }
                Map tableFiles = fileModifyTimesMap.get(tableName);
                tableFiles.put(pathStr, fileStatus[i].getModificationTime());
            }
        }
    }

    private static void getModifyTime(String fsPath, String path, Map<String, Map<String, Long>> fileModifyTimesMap)
    {
        FileSystem fs;
        conf.set("fs.defaultFS", fsPath);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            fs = FileSystem.get(conf);
            getModifyTime(new Path(path), fs, fileModifyTimesMap);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static JsonObject LoadJson(String filePath)
    {
        InputStream inputStream = Util.loadInputStream(filePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            String line = bufferedReader.readLine();
            if (line == null) {
                return null;
            }
            JsonObject jsonElements = jsonParser.parse(line).getAsJsonObject();
            bufferedReader.close();
            return jsonElements;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean getNeedUpdateTableAndPaths(JsonObject root, Map<String, Map<String, Long>> fileModifyTimesMap, Set<String> schedulerOutSet, List<String> tablePaths, List<String> tables)
    {
        for (Map.Entry<String, Map<String, Long>> table : fileModifyTimesMap.entrySet()) {
            String tableName = table.getKey();
            Map<String, Long> lastTableInfoMap = new HashMap<>();

            boolean shouldModified = false;

            if (root == null || root.has(tableName) == false) {
                shouldModified = true;
            }
            else {
                JsonObject secondRoot = root.getAsJsonObject(tableName);
                for (Map.Entry<String, Long> tablePathInfo : table.getValue().entrySet())
                {
                    String path = tablePathInfo.getKey();
                    long updateTime = tablePathInfo.getValue();
                    if (secondRoot.has(path) && Long.compare(secondRoot.get(path).getAsLong(), updateTime) == 0)
                    {
                        shouldModified = false;
                        log.info("table path（" + path + ") has not updated ,table (" + tableName + ") will not update ");
                    }
                    lastTableInfoMap.put(path, updateTime);
                }
            }
            if (shouldModified == true && schedulerOutSet.contains(tableName)) {
                Set<String> eachTablePaths = table.getValue().keySet();
                log.info("table （" + tableName + ") will add to mr job immediately");

                tablePaths.addAll(eachTablePaths);
                tables.add(tableName);
            }
            else {
                fileModifyTimesMap.put(tableName, lastTableInfoMap);
            }
        }
        return !(tablePaths.isEmpty() || tablePaths.isEmpty());
    }

    private static void updateLastJson(String lastJsonPath, Map<String, Map<String, Long>> fileModifyTimesMap)
    {
        PrintStream outStream = Util.writeFile(lastJsonPath);
        Gson gson = new Gson();
        outStream.println(gson.toJson(fileModifyTimesMap));
        outStream.close();
    }

    private static void writeFilterPath(String resultPath, List<String> tablePathList)
    {
        PrintStream outStream = Util.writeFile(resultPath);
        for (String path : tablePathList) {
            outStream.println(path);
        }
        log.info("now check out " + tablePathList.size() + " table paths ");
        outStream.close();
    }

    private static void writeFilterTable(String tablePath, List<String> tables)
    {
        PrintStream outStream = Util.writeFile(tablePath);
        for (String table : tables) {
            outStream.print(table);
            outStream.print(" ");
        }
        outStream.close();
    }

    public static void main(String[] args)
    {
        if (args.length != 6) {
            System.out.println("Usage: fs csvPath scheduler.out last.json tablePath.result tables.result");
        }
        String fs = args[0];
        String csvPath = args[1];
        String schedulerOut = args[2];

        String lastJsonPath = args[3];
        String tablesPathOut = args[4];
        String tablesNameOut = args[5];

        Map<String, Map<String, Long>> fileModifyTimesMap = new HashMap<>();
        Set<String> schedulerOutSet = new HashSet<>();

        // load scheduler Out
        loadSchedulerOutPut(schedulerOut, schedulerOutSet);

        // get file updateTimes

        getModifyTime(fs, csvPath, fileModifyTimesMap);

        // load last file UpdateTimes
        JsonObject lastJsonRoot = LoadJson(lastJsonPath);

        // get update Tables and Paths
        List<String> resultTablePathList = new ArrayList<>();
        List<String> resultTables = new ArrayList<>();
        boolean hasUpdateTables = getNeedUpdateTableAndPaths(lastJsonRoot, fileModifyTimesMap, schedulerOutSet, resultTablePathList, resultTables);

        // write out filter path
        writeFilterPath(tablesPathOut, resultTablePathList);

        // write out filter table
        writeFilterTable(tablesNameOut, resultTables);

        // Update tables
        if (hasUpdateTables) {
            updateLastJson(lastJsonPath, fileModifyTimesMap);
        }
        else {
            log.info("no updated table ");
        }

    }
}
