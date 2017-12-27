package tv.freewheel.reporting.matcher;

import org.quartz.impl.triggers.CronTriggerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
/**
 * each table has scheduler , in each hour candidate scheduled table out
 */
public class DimensionHiveSchedulerFilter
{
    private static final  String SCHEDULE_NAME = "table-scheduler";
    private static final  String SCHEDULE_GROUP = "scheduler-group";
    private static final  String SCHEDULE_GROUP_SPLIT = ":";
    private static Logger log = LoggerFactory.getLogger(DimensionHiveSchedulerFilter.class);
    private static final int AheadTime = 5;
    private static Date getCurrentDay()
    {
        Date date = new Date();
        return new Date(date.getYear(), date.getMonth(), date.getDay());
    }

    private static Date getNowHour()
    {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MINUTE, -AheadTime);
        return c.getTime();
    }

    public static void main(String[] args) throws ParseException, IOException
    {
        if (args.length != 2) {
            System.out.println("Usage: scheduler.list OutPath");
            return;
        }
        InputStream inputStream = Util.loadInputStream(args[0]);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        Date start = getCurrentDay();

        Date now = getNowHour();

        String schedulerOutFile = args[1];
        PrintStream outStream = Util.writeFile(schedulerOutFile);

        String tableSchedulerLine;
        int candidateTablesCount = 0;
        while ((tableSchedulerLine = bufferedReader.readLine()) != null) {
            log.info("parse current table : " + tableSchedulerLine);
            String[] array = tableSchedulerLine.split(SCHEDULE_GROUP_SPLIT);
            String table = array[0];
            String time = array[1];

            CronTriggerImpl cronTrigger = new CronTriggerImpl(SCHEDULE_NAME, SCHEDULE_GROUP, time);
            cronTrigger.setStartTime(start);
            Date nextExecDate = cronTrigger.getFireTimeAfter(now);

            if (nextExecDate != null) {
                if (Math.abs(nextExecDate.getTime() - now.getTime()) / 1000 <= AheadTime * 60) {
                    outStream.println(table);
                    candidateTablesCount++;
                    log.info("current table (" + table + ") gone to runPoint , will add to scheduler.out");
                }
                else {
                    log.info("current table (" + table + ") has not gone to runPoint , next run time at " + nextExecDate);
                }
            }
        }
        log.info("now check out " + candidateTablesCount + " tables , add to runPoint");
        outStream.close();

    }
}
