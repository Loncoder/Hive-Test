package tv.freewheel.reporting.matcher;

/**
 * Created by ysun on 12/15/15.
 */

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import tv.freewheel.reporting.input.BinaryLogCombineInputFormat;
import tv.freewheel.reporting.util.Constants;

public class RequestAnalyzer
{
    private String tmpSink;
    private Job job;
    private String source;
    private String sink;

    public RequestAnalyzer(String processor, Properties props) throws Exception
    {
        this(processor, props, new Configuration());
    }

    public RequestAnalyzer(String processor, Properties props, Configuration conf) throws Exception
    {
        if (!props.containsKey("source")) {
            throw new RuntimeException("source must be set");
        }
        if (!props.containsKey("sink")) {
            throw new RuntimeException("sink must be set");
        }

        String mode = props.getProperty("mode", "auto").toLowerCase();
        String startDate = props.getProperty("startDate", "");
        String endDate = props.getProperty("endDate", "");

        if (mode.equals("auto")) {
            String yesterday = DateTime.now(DateTimeZone.UTC).minusDays(2).toString("yyyyMMdd");
            startDate = yesterday;
            endDate = DateTime.now(DateTimeZone.UTC).toString("yyyyMMdd");
        }
        if (startDate.length() == 8) {
            startDate += "00";
        }

        if (endDate.length() == 8) {
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
        String networks = (props.getProperty("networks", ""));

        job.getConfiguration().set("startDate", startDate);
        job.getConfiguration().set("endDate", endDate);
        job.getConfiguration().setInt("ahead", 1);
        job.getConfiguration().setInt("delay", 0);
        job.getConfiguration().set("networks", networks);
        job.setNumReduceTasks(1);
        job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);

        // for BinaryLogCombineInputFormat to skip ack files
        job.getConfiguration().setBoolean("tv.freewheel.reporting.matcher.sampling", true);

        DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("yyyyMMddHH").withZoneUTC();
        DateTime startDatetime = DateTime.parse(startDate, dtFormatter);
        DateTime endDatetime = DateTime.parse(endDate, dtFormatter);
        if (startDatetime.isAfter(endDatetime)) {
            throw new RuntimeException("startDate can't be later than endDate.");
        }
    }

    public void run() throws Exception
    {
        job.setJobName("request-counter");
        job.setJarByClass(RequestAnalyzer.class);
        job.setMapperClass(RequestAnalyzerMapper.class);
        job.setReducerClass(RequestAnalyzerReducer.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setInputFormatClass(BinaryLogCombineInputFormat.class);

        FileInputFormat.setMaxInputSplitSize(job, 1024 * 1024 * 1024);
        FileInputFormat.setInputPaths(job, source);
        tmpSink = sink + System.nanoTime();
        FileOutputFormat.setOutputPath(job, new Path(tmpSink));

        if (job.waitForCompletion(true)) {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            cleanup();
            FileUtil.copy(fs, new Path(tmpSink), fs, new Path(sink), true, job.getConfiguration());
        }
        else {
            throw new Exception("log-analyzer failed!");
        }
    }

    private void cleanup() throws Exception
    {
        FileSystem.get(job.getConfiguration()).delete(new Path(sink), true);
    }

    public static void main(String[] args) throws Exception
    {
        String source = args[0];
        String sink = args[1];
        String startDate = args[2];
        String endDate = args[3];
        String queue = args[4];

        System.out.println("source" + ": " + source);
        System.out.println("sink" + ": " + sink);
        System.out.println("startDate" + ": " + startDate);
        System.out.println("endDate" + ": " + endDate);
        System.out.println("queue" + ": " + queue);

        Properties props = new Properties();
        props.setProperty(Constants.SOURCE, source);
        props.setProperty(Constants.SINK, sink);
        props.setProperty(Constants.START_DATE, startDate);
        props.setProperty(Constants.END_DATE, endDate);

        props.setProperty("-D" + JobContext.QUEUE_NAME, queue);

        new RequestAnalyzer("request-counter", props).run();
    }
}
