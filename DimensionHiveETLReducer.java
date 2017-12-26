package tv.freewheel.reporting.matcher;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ysun on 6/27/17.
 */
public class DimensionHiveETLReducer extends Reducer<Text, LongWritable, Text, LongWritable>
{
    Text key = new Text();
    LongWritable v = new LongWritable();

    @Override
    public void reduce(Text id, Iterable<LongWritable> count, Context context)
            throws IOException, InterruptedException
    {
        Long cnt = 0L;
        for (LongWritable i : count) {
            cnt += i.get();
        }
        key.set(id.toString());
        v.set(cnt);
        context.write(key, v);
        System.out.println(id.toString() + ": " + cnt);
    }
}
