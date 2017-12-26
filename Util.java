package tv.freewheel.reporting.matcher;

import java.io.InputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Util
{
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
}
