package tv.freewheel.reporting.matcher;

/**
 * Created by ysun on 12/15/15.
 */

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class LogAnalyzerReducer extends Reducer<Text, LongWritable, Text, LongWritable>
{
    private MultipleOutputs<Text, LongWritable> outputs;
    private Long outputThreshold;
    private double sampleRate;

    @Override
    public void setup(Context context)
    {
        outputs = new MultipleOutputs<>(context);
        outputThreshold = context.getConfiguration().getLong("outputThreshold", 2000000L);
        sampleRate = context.getConfiguration().getDouble("sampleRate", 1.0);
    }

    @Override
    public void reduce(Text id, Iterable<LongWritable> count, Context context)
            throws IOException, InterruptedException
    {
        Long cnt = 0L;
        for (LongWritable i : count) {
            cnt += i.get();
        }
        cnt = Math.round(cnt / sampleRate); //restore to estimated original count
        if (cnt > outputThreshold) {
            String filename = id.toString().substring(0, 8);
            // example: 2015121620-380990;380990=72102140
            outputs.write(filename, id, cnt);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
        outputs.close();
    }
}
