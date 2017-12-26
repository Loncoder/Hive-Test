package tv.freewheel.reporting.matcher;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class DimensionHiveETLRecordReader extends RecordReader<LongWritable, Text>
{
    private LineRecordReader reader = new LineRecordReader();
    private LongWritable longWritable = new LongWritable(0L);

    public DimensionHiveETLRecordReader(CombineFileSplit split,
            TaskAttemptContext context, Integer index) throws IOException
    {
        longWritable.set(index);
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
        return reader.nextKeyValue();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException
    {
        return longWritable;
        //        return reader.getCurrentKey();
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException
    {
        return reader.getCurrentValue();
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
