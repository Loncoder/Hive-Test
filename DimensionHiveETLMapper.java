package tv.freewheel.reporting.matcher;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class DimensionHiveETLMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
    private Text word = new Text();
    private MultipleOutputs<Text, LongWritable> output;
    private StringBuilder sb = new StringBuilder();

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
        output = new MultipleOutputs<>(context);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
        output.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        InputSplit is = context.getInputSplit();
        Path filePath;
        if (is instanceof FileSplit) {
            filePath = ((FileSplit) is).getPath();
        }
        else if (is instanceof CombineFileSplit) {
            filePath = ((CombineFileSplit) is).getPath((int) key.get());
        }
        else {
            throw new RuntimeException("Invalid input split.");
        }
        String val = value.toString();
        sb.append(val);
        String tableName = filePath.getName().replaceAll("\\.gz", "");
        boolean isAEnd = val.endsWith("\"");
        boolean isBEnd = val.endsWith("\\N");
        if ((isAEnd || isBEnd) && isLineComplete(sb.toString())) {
            word.set(handleNullValue(sb.toString()));
            output.write(tableName.replaceAll("_", "4"), word, NullWritable.get(), tableName + "/" + tableName);
            sb.delete(0, sb.length());
            context.getCounter("type", "normal write out").increment(1);
            context.write(new Text(tableName), new LongWritable(1));
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

    public static String handleNullValue(String line)
    {
        return line.replaceAll("^\\\\N,", "\"\\\\\\\\N\",").replaceAll(",\\\\N", ",\"\\\\\\\\N\"");
    }
}
