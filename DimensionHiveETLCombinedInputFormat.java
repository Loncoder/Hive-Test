package tv.freewheel.reporting.matcher;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Created by ysun on 6/27/17.
 */
public class DimensionHiveETLCombinedInputFormat extends CombineFileInputFormat<LongWritable, Text>
{
    @Override public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException
    {
        return new CombineFileRecordReader<>(
                (CombineFileSplit) split, context,
                DimensionHiveETLRecordReader.class);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file)
    {
        return false;
    }
}
