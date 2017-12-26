package tv.freewheel.reporting.matcher;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
@Deprecated
public class DimensionTransformerRecordReader extends RecordReader<Text, Text>
{
    private LineRecordReader reader = new LineRecordReader();
    private Text value = new Text();
    private Text key = new Text();
    private String table;

    public DimensionTransformerRecordReader(CombineFileSplit split,
            TaskAttemptContext context, Integer index) throws IOException
    {
        Path file = split.getPath(index);
        table = file.getName();
        if (table.endsWith("gz")) {
            table = table.substring(0, table.indexOf(".gz"));
        }
        key = new Text(table);
        FileSplit fileSplit = new FileSplit(split.getPath(index), 0, split.getLength(index), split.getLocations());
        reader.initialize(fileSplit, context);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        String line = "";
        try {
            while (reader.nextKeyValue()) {
                line += reader.getCurrentValue().toString();
                if ((line.endsWith("\\N") || line.endsWith("\"")) && isLineComplete(line)) {
                    value.set(line);
                    return true;
                }
                line += "\n";
            }
            return false;
        }
        catch (Exception e) {
            String err = String.format("DimensionTransformerRecordReader: %s:%s:%s.",
                    table, reader.getCurrentKey().toString(), line);
            throw new RuntimeException(err);
        }
    }

    private boolean isLineComplete(String line)
    {
        boolean enclosed = true;
        for (int i = 0; i < line.length(); i++) {
            switch (line.charAt(i)) {
            case '"':
                enclosed = !enclosed;
                break;
            case '\\':
                i += 1;
                break;
            default:
                break;
            }
        }

        return enclosed;
    }

    @Override
    public Text getCurrentKey() throws IOException,
            InterruptedException
    {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException
    {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException
    {
        reader.close();
    }
}
