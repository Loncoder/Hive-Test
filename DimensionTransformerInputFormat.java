package tv.freewheel.reporting.matcher;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
@Deprecated
public class DimensionTransformerInputFormat extends CombineFileInputFormat<Text, Text>
{
    @Override public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException
    {
        return new CombineFileRecordReader<>(
                (CombineFileSplit) split, context,
                DimensionTransformerRecordReader.class);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file)
    {
        return false;
    }

    @Override
    protected List<FileStatus> listStatus(JobContext context) throws IOException
    {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        String input = getInputPaths(context)[0].toString();
        List<FileStatus> result = new LinkedList<>();

        int tableCSV = 0;
        String tables = context.getConfiguration().get("dimensionTables", "");
        Set<String> tableLU = new HashSet<>();
        Collections.addAll(tableLU, tables.split(";"));

        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(new Path(input), true);
        while (iter.hasNext()) {
            LocatedFileStatus status = iter.next();
            if (status.isFile() && status.getPath().getName().startsWith("d_")) {
                // skip 0 size file
                if (status.getLen() == 0) {
                    System.out.println(String.format("Skip zero size file: %s", status.getPath()));
                    continue;
                }
                // check table defined in DDL
                String name = status.getPath().getName();
                String tableName = name.contains(".") ? name.split("\\.", 2)[0] : name;
                if (!tableLU.contains(tableName)) {
                    System.out.println(String.format("Schema not found in ddl: %s.", status.getPath()));
                    continue;
                }
                // add to splits
                result.add(status);
                tableCSV++;
            }
        }
        System.out.println(String.format("FileInputFormat: DDL tables %d, CSV tables %d.", tableLU.size(), tableCSV));
        return result;

    }
}
