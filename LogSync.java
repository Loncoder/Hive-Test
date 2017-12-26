package tv.freewheel.reporting.matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogSync
{
    public class CallableReport
    {
        public int fileSyncNum;
        public long fileSyncSize;

        public CallableReport()
        {
            fileSyncNum = 0;
            fileSyncSize = 0L;
        }
    }

    private class Task implements Callable<CallableReport>
    {
        private String server;
        private FileSystem fs;
        private FilenameFilter filter;
        private Pattern pattern = Pattern.compile("[ack|bin]-.*-(\\d{10})\\d{4}\\.log.*");

        public Task(String server, FilenameFilter filter) throws IOException
        {
            this.server = server;
            this.filter = filter;
            this.fs = FileSystem.get(new Configuration());
        }

        @Override
        public CallableReport call()
        {
            CallableReport cr = new CallableReport();
            try {
                for (File file : new File(server + "/binary/").listFiles(filter)) {
                    Matcher matcher = pattern.matcher(file.getName());
                    if (matcher.find()) {
                        String datetime = matcher.group(1);
                        Path src = new Path(file.getCanonicalPath());
                        Path dst = new Path(sink + "/" + datetime + "/" + file.getName());
                        if (!fs.exists(dst)) {
                            fs.copyFromLocalFile(src, dst);
                            cr.fileSyncNum += 1;
                            cr.fileSyncSize += fs.getFileStatus(dst).getLen();
                        }
                    }
                }
            }
            catch (Exception e) {
                System.err.println(e.getLocalizedMessage());
            }

            return cr;
        }
    }

    private String root; ///mnt/log-ads-new
    private String startDate;
    private String endDate;
    private String sink;
    private int executorNum;
    private boolean includeArchive;
    private ExecutorService executor;
    private ExecutorCompletionService<CallableReport> ecs;

    public LogSync(String processor, Properties props) throws Exception
    {
        DateTime datetime = new DateTime(DateTimeZone.UTC);
        if (!props.containsKey("root") || !props.containsKey("sink")) {
            throw new RuntimeException("root and sink must provide.");
        }
        root = props.getProperty("root");
        sink = props.getProperty("sink");
        startDate = props.getProperty("startDate", datetime.minusDays(3).toString("YYYYMMddHH"));
        endDate = props.getProperty("endDate", datetime.toString("YYYYMMddHH"));
        includeArchive = props.containsKey("includeArchive");
        executorNum = Integer.parseInt(props.getProperty("executors", "40"));
        executor = Executors.newFixedThreadPool(executorNum);
        ecs = new ExecutorCompletionService<CallableReport>(executor);
        System.out.printf("LogSync: startData-%s, endData-%s.\n", startDate, endDate);
        System.out.printf("LogSync: %d executors.\n", executorNum);
    }

    public void run() throws Exception
    {
        FilenameFilter filter = new FilenameFilter()
        {
            Pattern pattern = Pattern.compile("[ack|bin]-.*-(\\d{10})\\d{4}\\.log.*");

            @Override
            public boolean accept(File dir, String name)
            {
                if (name.startsWith("bin") || name.startsWith("ack")) {
                    Matcher matcher = pattern.matcher(name);
                    if (matcher.find()) {
                        String datetime = matcher.group(1);
                        if (datetime.compareTo(startDate) >= 0 && datetime.compareTo(endDate) <= 0) {
                            return true;
                        }
                    }
                }

                return false;
            }
        };

        int num = 0;
        System.out.printf("submitting: %s\n", root + "/current");
        for (File server : new File(root + "/current").listFiles()) { ///mnt/log-ads-new/current/*
            ecs.submit(new Task(server.getAbsolutePath(), filter));
            num += 1;
        }

        if (includeArchive) {
            String startDay = String.format("%s-%s-%s", startDate.substring(0, 4), startDate.substring(4, 6), startDate.substring(6, 8));
            String endDay = String.format("%s-%s-%s", endDate.substring(0, 4), endDate.substring(4, 6), endDate.substring(6, 8));
            for (File date : new File(root + "/archive-dates").listFiles()) {
                if (date.getName().compareTo(startDay) >= 0 && date.getName().compareTo(endDay) <= 0) {
                    System.out.printf("archive submitting: %s\n", date.getAbsolutePath());
                    for (File server : date.listFiles()) {
                        ecs.submit(new Task(server.getAbsolutePath(), filter));
                        num += 1;
                    }
                }
                else {
                    System.out.printf("archive skip %s\n", date.getAbsolutePath());
                }
            }

        }
        System.out.printf("LogSync: %d tasks submitted.\n", num);

        int successNum = 0;
        long successSize = 0L;

        int logStep = num / 40;
        int lastLogNum = 0;
        long lastLogSize = 0L;
        long lastLogTs = 0L;
        long startTs = 0L;
        lastLogTs = System.currentTimeMillis();
        startTs = lastLogTs;
        for (int i = 0; i < num; i++) {
            CallableReport cr = ecs.take().get();
            successNum += cr.fileSyncNum;
            successSize += cr.fileSyncSize;
            if (i - lastLogNum == logStep) {
                int process = i * 100 / num; //just accurate to integer
                double speed = (successSize - lastLogSize) / (System.currentTimeMillis() - lastLogTs + 1); // KB/sec
                System.out.printf("LogSync: %d%% ... %d tasks finished, %d new file ingested, speed %.1f KB/s.\n", process, i, successNum, speed);
                lastLogNum = i;
                lastLogSize = successSize;
                lastLogTs = System.currentTimeMillis();
            }
        }
        executor.shutdown();
        double speedAvr = successSize / (System.currentTimeMillis() - startTs);
        System.out.printf("LogSync Report: %d files(size:%d Bytes) have been sync to hdfs, average speed %.1f KB/s.\n", successNum, successSize,
                speedAvr);
    }

    public void cancel() throws Exception
    {
        executor.shutdown();
        System.out.println("LogSync has been killed");
    }

    public static void main(String[] args) throws Exception
    {
        Properties props = new Properties();
        String root = args[0];
        String sink = args[1];
        String startHour = args[2];
        String endHour = args[3];
        Boolean includeArchive = false;
        int numExecutor = 40;

        if (args.length > 4) {
            includeArchive = Boolean.parseBoolean(args[4]);
        }
        if (args.length > 5) {
            numExecutor = Integer.valueOf(args[5]);
        }

        props.setProperty("root", root);
        System.out.println("root: " + root);

        props.setProperty("sink", sink);
        System.out.println("sink: " + sink);

        props.setProperty("startDate", startHour);
        System.out.println("start hour: " + startHour);

        props.setProperty("endDate", endHour);
        System.out.println("end hour: " + endHour);

        if (includeArchive) {
            props.setProperty("includeArchive", "true");
            System.out.println("include archive: true");
        }
        else {
            System.out.println("include archive: false");
        }

        props.setProperty("executors", Integer.toString(numExecutor));
        System.out.println("executors: " + Integer.toString(numExecutor));

        new LogSync("", props).run();
    }
}
