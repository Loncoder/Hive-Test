package tv.freewheel.reporting.matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;

/**
 * unify csv files as external tables in Hive
 */

public class DimensionHiveETL extends Configured implements Tool
{
    public Job job;
    public FileSystem fs;
    private String sink;
    private String runningSink;
    private String scheduledTableLoadPath;
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

    public DimensionHiveETL(String name, Properties props) throws Exception
    {
        this(name, props, new Configuration());
    }

    public DimensionHiveETL(String name, Properties props, Configuration conf) throws Exception
    {
        if (props.containsKey("sink")) {
            sink = props.getProperty("sink") + "/" + CURRENT;
            runningSink = props.getProperty("sink") + "/" + WORKING;
        }
        else {
            throw new RuntimeException("sink must be set");
        }

        if (props.containsKey("scheduledLoadPath")) {
            scheduledTableLoadPath = props.getProperty("scheduledLoadPath");
        }

        System.out.println("Sink final: " + sink);
        System.out.println("Sink when running: " + runningSink);
        System.out.println("scheduledLoadPath: " + scheduledTableLoadPath);

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

        for (Object objKey : props.keySet()) {
            String key = (String) objKey;
            if (key.startsWith("-D")) {
                String mrConfig = key.substring(2);
                String configValue = props.getProperty(key);
                conf.set(mrConfig, configValue);
                System.out.println(String.format("Conf set %s to %s", mrConfig, configValue));
            }
        }
    }

    private Path[] loadScheduledPath()
    {
        List<Path> schedulerPaths = new ArrayList<>();
        File file = new File(scheduledTableLoadPath);
        try {
            InputStream inputStream = new FileInputStream(file);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String path;
            while ((path = bufferedReader.readLine()) != null) {
                schedulerPaths.add(new Path(path));
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return schedulerPaths.toArray(new Path[0]);
    }

    @Override
    public int run(String [] args) throws Exception
    {
        int result = job.waitForCompletion(true) ? 0 : 1;
        if (result == 0) {
            fs.delete(new Path(sink), true);
            fs.rename(new Path(runningSink), new Path(sink));
        }
        else {
            fs.delete(new Path(runningSink), true);
            throw new Exception("dimension ETL failed!");
        }
        return result;
    }

    public static void main(String[] args)
    {
        if (args.length != 2) {
            System.out.println("sink, scheduledLoadPath");
            return;
        }
        Properties properties  = new Properties();
        properties.put("sink", args[0]);
        properties.put("scheduledLoadPath", args[1]);
        try {
            DimensionHiveETL etl = new DimensionHiveETL("DimensionHiveETL", properties);
            etl.run(args);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
