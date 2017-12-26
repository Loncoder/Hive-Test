package tv.freewheel.reporting.matcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
@Deprecated
public class DimensionTransformerMapper extends Mapper<Text, Text, Text, Text>
{
    private long partitionSize;
    private Map<String, Integer> tablePartition;
    private int roundRobin;

    @Override
    public void setup(Context context) throws IOException
    {
        partitionSize = context.getConfiguration().getLong("partitionSize", DimensionTransformer.OUTPUT_PARTITION_SIZE);
        System.out.println(String.format("set output partition size: %d.", partitionSize));
        if (partitionSize <= 0) {
            throw new RuntimeException("output partition size must be positive.");
        }
        tablePartition = new HashMap<>();
        loadSize(context);
        roundRobin = 0;
    }

    @Override
    public void cleanup(Context contet) throws IOException, InterruptedException
    {
    }

    @Override
    public void map(Text table, Text line, Context context) throws IOException, InterruptedException
    {
        int sharding = tablePartition.getOrDefault(table.toString(), 1);
        Text key = new Text(String.format("%s#%d", table, roundRobin++ % sharding));

        context.write(key, line);
    }

    private void loadSize(Context context) throws IOException
    {
        String sizeFile = context.getConfiguration().get("sizeFile");
        Path path = new Path(sizeFile);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        if (!fs.exists(path)) {
            return;
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        try {
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] record = line.split("=");
                if (record.length != 2) {
                    System.out.println("Malformed line: " + line);
                    continue;
                }
                String key = record[0];
                if (key.endsWith(".gz")) {
                    key = key.substring(0, key.indexOf(".gz"));
                }
                int splitN = (int) Math.ceil(Long.valueOf(record[1]) / (double) partitionSize);
                tablePartition.put(key, splitN);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            br.close();
        }
    }
}
