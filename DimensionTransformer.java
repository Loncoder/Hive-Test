package tv.freewheel.reporting.matcher;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import tv.freewheel.reporting.ddl.MysqlDDL;
import tv.freewheel.reporting.ddl.TableElement;
@Deprecated
public class DimensionTransformer
{
    private Job job;
    private String sink;
    /**
     * path:
     * {sink}/{CURRENT_DIR}: final path for parquet
     * {sink}/{NEW_DIR}: temporary data when running
     * {sink}/{OLD_DIR}: temporary data for swapping
     */
    public static final String CURRENT_DIR = "current";
    public static final String NEW_DIR = "_NEW";
    public static final String OLD_DIR = "_OLD";
    public static final long OUTPUT_PARTITION_SIZE = 250 * 1024 * 1024;
    public static final long MAX_INPUT_SPLIT_SIZE = 256 * 1024 * 1024;

    public static void main(String[] args) throws Exception
    {
        Properties props = new Properties();
        props.setProperty("source", args[0]);
        props.setProperty("sink", args[1]);
        props.setProperty("reducers", args[2]);
        props.setProperty("splitLines", args[3]);
        props.setProperty("ddlPath", args[4]);
        DimensionTransformer transformer = new DimensionTransformer("transformer", props);
        transformer.run();
    }

    public DimensionTransformer(String name, Properties props) throws Exception
    {
        this(name, props, new Configuration());
    }

    public DimensionTransformer(String name, Properties props, Configuration conf) throws Exception
    {
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
        Configuration config = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        int reducerNum = Integer.parseInt(props.getProperty("reducers", "40"));
        if (props.containsKey("source")) {
            DimensionTransformerInputFormat.setInputPaths(job, props.getProperty("source"));
        }
        else {
            throw new RuntimeException("source must be set");
        }

        if (props.containsKey("sink")) {
            sink = props.getProperty("sink");
            AvroParquetOutputFormat.setOutputPath(job, new Path(sink, NEW_DIR));
        }
        else {
            throw new RuntimeException("sink must be set");
        }

        if (!props.containsKey("ddlPath")) {
            throw new RuntimeException("ddl must be set");
        }

        // ddlPath should be a directory, in which contains all the ddl files
        Path ddlPath = new Path(props.getProperty("ddlPath"));
        if (!fs.isDirectory(ddlPath)) {
            throw new RuntimeException("DDL path is not a directory or not exist:" + ddlPath.toString());
        }
        config.set("ddlPath", props.getProperty("ddlPath"));

        if (!props.containsKey("partitionSize")) {
            config.setLong("partitionSize", OUTPUT_PARTITION_SIZE);
        }
        else {
            config.setLong("partitionSize", Long.parseLong(props.getProperty("partitionSize")));
        }
        config.set("sizeFile", props.get("source") + "/_SIZE");

        job.setJarByClass(getClass());
        job.setInputFormatClass(DimensionTransformerInputFormat.class);
        job.setMapperClass(DimensionTransformerMapper.class);
        job.setReducerClass(DimensionTransformerReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(reducerNum);
        DimensionTransformerInputFormat.setMaxInputSplitSize(job, MAX_INPUT_SPLIT_SIZE);
        AvroParquetOutputFormat.setCompressOutput(job, true);
        AvroParquetOutputFormat.setCompression(job, CompressionCodecName.GZIP);
        Map<String, TableElement> tableDeclares = new HashMap<>();

        // support multiple ddl files.
        for (FileStatus ddl : fs.listStatus(ddlPath)) {
            Map<String, TableElement> declares;
            if (ddl.isFile() && ddl.getPath().getName().toLowerCase().endsWith(".ddl")) {
                declares = MysqlDDL.parse(new InputStreamReader(fs.open(ddl.getPath())));
                for (String tbName : declares.keySet()) {
                    if (tableDeclares.containsKey(tbName)) {
                        throw new RuntimeException("Error: Duplicated definition for table " + tbName);
                    }
                    tableDeclares.put(tbName, declares.get(tbName));
                }
            }
        }

        int index = 1;
        List<String> tables = new ArrayList<>();
        for (String tbName : tableDeclares.keySet()) {
            if (tbName.startsWith("d_")) {
                System.out.println(String.format("[%02d]Found define for table %s.", index, tbName));
                index++;
                tables.add(tbName);
                MultipleOutputs.addNamedOutput(job, tbName.replace('_', '4'), AvroParquetOutputFormat.class,
                        NullWritable.class, NullWritable.class);
            }
        }
        job.getConfiguration().set("dimensionTables", String.join(";", tables));

    }

    public void run() throws Exception
    {
        if (job.waitForCompletion(true)) {
            update();
        }
        else {
            cleanup();
            throw new Exception("dimension transform failed!");
        }
    }

    public void cancel() throws Exception
    {
        if (job != null && !job.isComplete()) {
            job.killJob();
            cleanup();
        }
    }

    private void update() throws IOException
    {
        System.out.println("updating path...");
        FileSystem fs = FileSystem.get(job.getConfiguration());
        fs.rename(new Path(sink, CURRENT_DIR), new Path(sink, OLD_DIR));
        fs.rename(new Path(sink, NEW_DIR), new Path(sink, CURRENT_DIR));
        fs.delete(new Path(sink, OLD_DIR), true);
    }

    private void cleanup() throws IllegalArgumentException, IOException
    {
        System.out.println("cleaning up...");
        FileSystem.get(job.getConfiguration()).delete(new Path(sink, NEW_DIR), true);
    }
}
