package tv.freewheel.reporting.matcher;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
/**
 * unify csv files as external tables in Hive
 */

public class DimensionHiveETL
{
    public Job job;
    public FileSystem fs;
    private String sink;
    private String runningSink;
    private String scheduledLoadPath;
    private String scheduledLoadTable;

    /**
     * input path:
     * {source}
     * <p>
     * output path:
     * {sink}: final path for csv
     * <p>
     * {sink}/{WORKING}: temporary data when running
     */
    private static final String CURRENT = "current";
    private static final String WORKING = "_running";
    private static final long MAX_INPUT_SPLIT_SIZE = 32 * 1024 * 1024;

    public DimensionHiveETL(String name, Properties props, Configuration conf) throws Exception
    {
        sink = props.getProperty("sink") + "/" + CURRENT;
        runningSink = props.getProperty("sink") + "/" + WORKING;
        scheduledLoadPath = props.getProperty("scheduledLoadPath");
        scheduledLoadTable = props.getProperty("scheduledLoadTable");
        System.out.println("Sink final: " + sink);
        System.out.println("Sink when running: " + runningSink);
        System.out.println("scheduledLoadPath: " + scheduledLoadPath);

        Job job = Job.getInstance(conf, getClass().getSimpleName());
        job.setJobName(name);
        job.setJarByClass(getClass());
        job.setInputFormatClass(DimensionHiveETLCombinedInputFormat.class);
        job.setMapperClass(DimensionHiveETLMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(DimensionHiveETLReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // simple reducer for table line count
        job.setNumReduceTasks(1);

        DimensionHiveETLCombinedInputFormat.setMaxInputSplitSize(job, MAX_INPUT_SPLIT_SIZE);
        System.out.println("Set default input split size: " + String.valueOf(MAX_INPUT_SPLIT_SIZE));

        FileSystem fs = FileSystem.get(conf);
        this.job = job;
        this.fs = fs;

        fs.delete(new Path(runningSink), true);

        Path[] candidatePath = loadScheduledPath();
        FileStatus[] fileStatuses = fs.listStatus(candidatePath);

        Set<String> out = new HashSet<>();
        for (FileStatus status : fileStatuses) {
            if (status.isFile() && status.getPath().getName().startsWith("d_")) {
                // skip 0 size file
                if (status.getLen() == 0) {
                    System.out.println(String.format("Skip zero size file: %s", status.getPath()));
                    continue;
                }
                String csvFileName = status.getPath().getName();
                String tableName = csvFileName.contains(".") ? csvFileName.split("\\.", 2)[0] : csvFileName;
                // the name of namedOutput can be only letters and numbers
                String outName = tableName.replaceAll("_", "4").replaceAll("\\.gz", "");

                if (!out.contains(outName)) {
                    MultipleOutputs.addNamedOutput(job, outName, TextOutputFormat.class, Text.class, LongWritable.class);
                    out.add(outName);
                    System.out.println("Added named output: " + outName);
                }
                FileInputFormat.addInputPath(job, status.getPath());
            }
        }
        FileOutputFormat.setOutputPath(job, new Path(runningSink));
    }

    private Path[] loadScheduledPath()
    {
        final List<Path> schedulerPaths = new ArrayList();
        Util.readFileContent(scheduledLoadPath, new Util.FileHandler() {
            @Override
            public void handlerLine(String path)
            {
                schedulerPaths.add(new Path(path));
            }
        });
        return schedulerPaths.toArray(new Path[0]);
    }

    private List<String> loadScheduleTable()
    {
        final List<String> scheduledTableLoadTables = new ArrayList();
        Util.readFileContent(scheduledLoadTable, new Util.FileHandler() {
            @Override
            public void handlerLine(String table)
            {
                scheduledTableLoadTables.add(table);
            }
        });
        return scheduledTableLoadTables;
    }

    public int run() throws Exception
    {
        List<String> scheduledTables = loadScheduleTable();
        int result = job.waitForCompletion(true) ? 0 : 1;
        if (result == 0) {
            for (String table : scheduledTables) {
                String sinkPath = sink + "/" + table;
                String tmpPath = runningSink + "/" + table;

                fs.delete(new Path(sinkPath), true);
                fs.rename(new Path(tmpPath), new Path(sinkPath));
                System.out.println("deleting path : " + sinkPath);
            }
        }
        else {
            fs.delete(new Path(runningSink), true);
            throw new Exception("dimension ETL failed!");
        }
        return result;
    }

    public static void main(String[] args)
    {
        if (args.length < 4) {
            System.out.println("sink, scheduledLoadPath ");
            return;
        }
        Properties properties  = new Properties();
        properties.put("sink", args[0]);
        properties.put("scheduledLoadPath", args[1]);
        properties.put("scheduledLoadTable", args[2]);
        Configuration conf = new Configuration();

        if (args.length == 4) {
            conf.set("mapreduce.job.queuename", args[3]);
        }
        try {
            DimensionHiveETL etl = new DimensionHiveETL("DimensionHiveETL", properties, conf);
            etl.run();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
