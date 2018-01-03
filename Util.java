package tv.freewheel.reporting.matcher;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Properties;

public class Util
{
    interface FileHandler
    {
        void handlerLine(String line);
    }

    public static void readFileContent(String path, FileHandler handler)
    {
        File file = new File(path);
        try {
            InputStream inputStream = new FileInputStream(file);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String data;
            while ((data = bufferedReader.readLine()) != null) {
                handler.handlerLine(data);
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static InputStream loadInputStream(String path)
    {
        File file = new File(path);
        try {
            InputStream inputStream = new FileInputStream(file);
            return inputStream;
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static PrintStream writeFile(String path)
    {
        File file = new File(path);
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintStream printStream = new PrintStream(file);
            return printStream;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Properties loadProperties(String path)
    {
        Properties properties = new Properties();
        try {
            properties.load(Util.class.getClassLoader().getResourceAsStream(path));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
