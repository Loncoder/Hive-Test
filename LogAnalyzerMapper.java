package tv.freewheel.reporting.matcher;

/**
 * Created by ysun on 12/15/15.
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Random;
import java.util.regex.Pattern;

import report.LogRecord.Request_Log_Record;
import tv.freewheel.reporting.util.Utils;

import com.google.protobuf.CodedInputStream;

public class LogAnalyzerMapper extends Mapper<String, byte[], Text, LongWritable>
{
    private Text key = new Text();
    private String upper;
    private String lower;
    private Pattern filter;
    private double sRate; //sample rate
    private Random rand; //for sampling

    @Override
    public void setup(Context context)
    {
        String startDate = context.getConfiguration().get("startDate", "19700101");
        String endDate = context.getConfiguration().get("endDate", "20491230");

        if (startDate.length() == 8) {
            startDate += "00";
        }

        if (endDate.length() == 8) {
            endDate += "23";
        }

        lower = String.valueOf(Utils.dateToTs(startDate));
        upper = String.valueOf(Utils.dateToTs(endDate) + 3600);
        filter = Pattern.compile("[^\\d;]");
        sRate = context.getConfiguration().getDouble("sampleRate", 1.0);
        rand = new Random();
        rand.setSeed(System.currentTimeMillis());
    }

    @Override
    public void map(String server, byte[] data, Context context) throws InterruptedException
    {
        if (sRate < rand.nextDouble()) {
            return;
        }
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int type = bb.getInt();
        int size = bb.getInt();
        try {
            // sampling only request count
            if (type == 2) {
                CodedInputStream blob = CodedInputStream.newInstance(data, 8, size);
                Request_Log_Record request = Request_Log_Record.parseFrom(blob);
                if (isValid(request.getTransactionId())) {
                    key.set(format(request.getNetworks(), server, request.getTransactionId()));
                    context.write(key, new LongWritable(1));
                }
            }
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
        }

    }

    private String format(String networks, String serverId, String transactionId) throws IOException
    {
        if (transactionId.length() < 10) {
            throw new IOException("Invalid transaction id: " + transactionId);
        }
        networks = networks.replaceAll("%3B|%253B|-3B", ";");
        if (filter.matcher(networks).find()) {
            throw new IOException("Invalid networks: " + networks);
        }
        try {
            String datetime = Utils.tsToDate(Long.parseLong(transactionId.substring(0, 10)));
            // split by distributor and CRO
            String[] parts = networks.split(";", 3);
            if (parts.length > 2) {
                networks = parts[0] + ";" + parts[1];
            }
            return String.format("%s-%s", datetime, networks);
        }
        catch (NumberFormatException e) {
            throw new IOException("Invalid transaction id: " + transactionId);
        }
    }

    private boolean isValid(String id)
    {
        return lower.compareTo(id) <= 0 && upper.compareTo(id) > 0;
    }
}
