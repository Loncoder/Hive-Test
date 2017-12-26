package tv.freewheel.reporting.matcher;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Created by ysun on 6/23/17.
 */
public class DimensionHiveETLInputFormat extends TextInputFormat
{
    @Override
    protected boolean isSplitable(JobContext context, Path file)
    {
        return false;
    }
}
