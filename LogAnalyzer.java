package tv.freewheel.reporting.matcher;
/**
 * Created by ysun on 12/15/15.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import tv.freewheel.reporting.input.BinaryLogCombineInputFormat;

import java.util.Properties;

public class LogAnalyzer
{
    private Job job;
    private String source;
    private String sink;
    private String sinkTemp;
    String startDate;
    String endDate;

    public LogAnalyzer(String processor, Properties props) throws Exception
    {
        this(processor, props, new Configuration());
    }

    public LogAnalyzer(String processor, Properties props, Configuration conf) throws Exception
    {
        if (!props.containsKey("source")) {
            throw new RuntimeException("source must be set");
        }
        if (!props.containsKey("sink")) {
            throw new RuntimeException("sink must be set");
        }

        String mode = props.getProperty("mode", "auto").toLowerCase();
        startDate = props.getProperty("startDate", "");
        endDate = props.getProperty("endDate", "");

        if (mode.equals("auto")) {
            String yesterday = DateTime.now(DateTimeZone.UTC).minusDays(2).toString("yyyyMMdd");
            startDate = yesterday;
            endDate = yesterday;
        }
        if (startDate.length() != 8 || endDate.length() != 8) { //by day
            throw new RuntimeException("startDate/endDate must formatted as yyyyMMdd");
        }
        else {
            startDate += "00";
            endDate += "23";
        }

        for (Object objKey : props.keySet()) {
            String key = (String) objKey;
            if (key.startsWith("-D")) {
                String mrConfig = key.substring(2);
                String configValue = props.getProperty(key);
                conf.set(mrConfig, configValue);
                System.out.println(String.format("Conf set %s to %s", mrConfig, configValue));
            }
        }

        job = Job.getInstance(conf);
        source = props.getProperty("source");
        sink = props.getProperty("sink");
        sinkTemp = sink + "/" + System.currentTimeMillis();

        double sampleRate = Double.valueOf(props.getProperty("sampleRate", "1.0"));
        // set sample rate to range [0.0001, 1.0]
        sampleRate = Math.min(sampleRate, 1.0);
        sampleRate = Math.max(sampleRate, 0.0001);
        System.out.printf("Set sample rate to %f .\n", sampleRate);

        long outputThreshold = Long.valueOf(props.getProperty("outputThreshold", "2000000"));
        System.out.printf("Set output threshold to %d.\n", outputThreshold);

        job.getConfiguration().set("startDate", startDate);
        job.getConfiguration().set("endDate", endDate);
        job.getConfiguration().setInt("ahead", 1);
        job.getConfiguration().setInt("delay", 0);
        job.getConfiguration().set("networks", props.getProperty("networks", ""));
        job.setNumReduceTasks(1);
        job.getConfiguration().setDouble("sampleRate", sampleRate);
        job.getConfiguration().setLong("outputThreshold", outputThreshold);
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "=");
        job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);

        // for BinaryLogCombineInputFormat to skip ack files
        job.getConfiguration().setBoolean("tv.freewheel.reporting.matcher.sampling", true);

        DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("yyyyMMddHH").withZoneUTC();
        DateTime startDatetime = DateTime.parse(startDate, dtFormatter);
        DateTime endDatetime = DateTime.parse(endDate, dtFormatter);
        if (startDatetime.isAfter(endDatetime)) {
            throw new RuntimeException("startDate can't be later than endDate.");
        }
        while (startDatetime.isBefore(endDatetime)) {
            // add named output, by day
            MultipleOutputs.addNamedOutput(job, startDatetime.toString("yyyyMMdd"), TextOutputFormat.class,
                    Text.class, LongWritable.class);
            System.out.printf("Added resolved sample date: %s.\n", startDatetime.toString("yyyyMMdd"));
            startDatetime = startDatetime.plusDays(1);
        }
    }

    public void cancel() throws Exception
    {
        if (job != null && !job.isComplete()) {
            job.killJob();
            cleanup();
        }
    }

    public void run() throws Exception
    {
        job.setJobName("log-analyzer-" + startDate + "-" + endDate);
        job.setJarByClass(LogAnalyzer.class);
        job.setMapperClass(LogAnalyzerMapper.class);
        job.setReducerClass(LogAnalyzerReducer.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setInputFormatClass(BinaryLogCombineInputFormat.class);

        FileInputFormat.setMaxInputSplitSize(job, 1024 * 1024 * 1024);
        FileInputFormat.setInputPaths(job, source);
        FileOutputFormat.setOutputPath(job, new Path(sinkTemp));

        if (job.waitForCompletion(true)) {
            update();
        }
        else {
            cleanup();
            throw new Exception("log-analyzer failed!");
        }
    }

    private void cleanup() throws Exception
    {
        FileSystem.get(job.getConfiguration()).delete(new Path(sinkTemp), true);
    }

    private void update() throws Exception
    {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        //20141231-r-00000
        for (FileStatus status : fs.listStatus(new Path(sinkTemp))) {
            if (status.isFile() && status.getPath().getName().matches("\\d{8}-r-.*")) {
                String newFileName = status.getPath().getName().substring(0, 8);
                FileUtil.copy(fs, status, fs, new Path(sink + "/" + newFileName), false, true, job.getConfiguration());
            }
        }
        cleanup();
    }

    public static void main(String[] args) throws Exception
    {
        Properties props = new Properties();
        String source = args[0];
        String sink = args[1];
        String mode = "manual";
        String startDate = args[2];
        String endDate = args[3];
        String sampleRate = args[4];

        if (args.length > 5) {
            for (int i = 5; i < args.length; i++) {
                if (args[i].startsWith("-D")) {
                    String[] mrProp = args[i].split("=");
                    props.setProperty(mrProp[0], mrProp[1]);
                    System.out.printf("Set %s to %s.\n", mrProp[0], mrProp[1]);
                }
            }
        }

        System.out.println("source" + ": " + source);
        System.out.println("sink" + ": " + sink);
        System.out.println("mode" + ": " + mode);
        System.out.println("startDate" + ": " + startDate);
        System.out.println("endDate" + ": " + endDate);
        System.out.println("sampleRate" + ": " + sampleRate);

        props.setProperty("source", source);
        props.setProperty("sink", sink);
        props.setProperty("mode", mode);
        props.setProperty("startDate", startDate);
        props.setProperty("endDate", endDate);
        props.setProperty("sampleRate", sampleRate);
        props.setProperty("-Dmapreduce.job.queuename", "data");

        new LogAnalyzer("", props).run();
    }
}
