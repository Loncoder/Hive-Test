package tv.freewheel.reporting.matcher;

/**
 * Created by ysun on 12/15/15.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import report.LogRecord.Request_Log_Record;
import tv.freewheel.reporting.helper.Flags;
import tv.freewheel.reporting.util.Utils;

import com.google.protobuf.CodedInputStream;

public class RequestAnalyzerMapper extends Mapper<String, byte[], Text, LongWritable>
{
    private Text key = new Text();
    private String upper;
    private String lower;
    private Pattern filter;
    private String networks; //sample rate
    private HashMap<Long, String> tzMap = null;

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
        networks = context.getConfiguration().get("networks");
        tzMap = loadFile("/user/eng/timezone/tz", context.getConfiguration());
    }

    @Override
    public void map(String server, byte[] data, Context context) throws InterruptedException
    {
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int type = bb.getInt();
        int size = bb.getInt();
        try {
            // sampling only request count
            if (type == 2) {
                CodedInputStream blob = CodedInputStream.newInstance(data, 8, size);
                Request_Log_Record request = Request_Log_Record.parseFrom(blob);
                if (isValid(request.getTransactionId()) && !isFiltered(request)) {
                    String txNetworkString = request.getNetworks();
                    txNetworkString = txNetworkString.replaceAll("%3B|%253B|-3B", ";");
                    if (filter.matcher(txNetworkString).find()) {
                        throw new IOException("Invalid networks: " + networks);
                    }
                    Set<String> txNetworks = new HashSet<>(Arrays.asList(txNetworkString.split(";")));
//                    String croNetwork = Long.toString(request.getRequest().getAssetChain().getContentRightOwner().getNetworkId());

                    for (String network : networks.split(",")) {
                        if (txNetworks.contains(network)) {
                            key.set(format(request.getTransactionId(), network, tzMap));
                            context.write(key, new LongWritable(1));
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }

    private String format(String transactionId, String network, HashMap<Long, String> tMap) throws IOException
    {
        if (transactionId.length() < 10) {
            throw new IOException("Invalid transaction id: " + transactionId);
        }

        try {
//            String time = new DateTime(Long.parseLong(transactionId.substring(0, 10)) * 1000, DateTimeZone.forID("America/New_York")).toString("yyyy-MM-dd HH:00:00");
            String time = new DateTime(Long.parseLong(transactionId.substring(0, 10)) * 1000, DateTimeZone.forID(tMap.get(Long.parseLong(network)))).toString("yyyy-MM-dd HH:00:00");
            return network + "\t" + time;
        }
        catch (NumberFormatException e) {
            throw new IOException("Invalid transaction id: " + transactionId);
        }
    }

    private HashMap<Long, String> loadFile(String file, Configuration conf)
    {
        System.out.println("BEGIN TO load " + file);
        HashMap<Long, String> tmpMap = new HashMap<>();
        BufferedReader reader = null;
        try {
            FileSystem fileSystem = FileSystem.newInstance(conf);

            Path path = new Path(file);
            if (!fileSystem.exists(path)) {
                System.out.println("File " + file + " does not exists");
                return null;
            }
            reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
            String line = null;

            while ((line = reader.readLine()) != null) {
                String[] strs = line.split(",");
                tmpMap.put(Long.parseLong(strs[0]), strs[1]);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        finally {
            if (null != reader) {
                try {
                    reader.close();
                }
                catch (Exception e) {
                }
            }
        }
        return tmpMap;
    }

    private boolean isValid(String id)
    {
        return lower.compareTo(id) <= 0 && upper.compareTo(id) > 0;
    }

    private boolean isFiltered(Request_Log_Record request)
    {
        return Flags.RequestFlag.isFiltered(request.getFlags());
    }
}
