package tv.freewheel.reporting.matcher;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Deprecated
public class DimensionSync
{
    private static final int GZIP_BUFFER = 1024;

    public class CallableReport
    {
        public long fileNum;
        public String dirName;
        Map<String, Long> filesSize;

        public CallableReport()
        {
            dirName = "";
            fileNum = 0L;
            filesSize = new HashMap<>();
        }
    }

    private class Task implements Callable<CallableReport>
    {
        private String srcRoot;
        private String sinkRoot;
        private FileSystem fs;
        private CallableReport cr = new CallableReport();
        private final int maxWalkDepth = 5;

        public Task(String src, String sink) throws IOException
        {
            this.srcRoot = src;
            this.sinkRoot = sink;
            this.fs = FileSystem.get(conf);
            this.cr.dirName = srcRoot;
        }

        @Override
        public CallableReport call()
        {
            try {
                walk(new File(srcRoot), 0);
            }
            catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
            return cr;
        }

        private void walk(File root, int depth) throws IOException
        {
            if (depth++ > maxWalkDepth) {
                System.out.println(String.format("Path walk too deep: %s", root.getAbsolutePath()));
                return;
            }
            if (root.isFile() && root.getName().startsWith("d_")) {
                sync(root.getAbsolutePath());
            }
            else if (root.isDirectory()) {
                File[] files = root.listFiles();
                if (files == null) {
                    return;
                }
                for (File f : files) {
                    walk(f, depth);
                }
            }
        }

        private String compressGZ(File src, boolean delete) throws IOException
        {
            File gzFile = new File(src.getAbsolutePath() + ".gz");
            FileInputStream fis = new FileInputStream(src);
            GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(gzFile));
            int count;
            byte[] data = new byte[GZIP_BUFFER];
            while ((count = fis.read(data, 0, GZIP_BUFFER)) != -1) {
                gos.write(data, 0, count);
            }
            fis.close();
            gos.finish();
            gos.flush();
            gos.close();
            if (delete) {
                src.delete();
            }
            return gzFile.getAbsolutePath();
        }

        private void sync(String absFilePath) throws IOException
        {
            boolean delSrc = false;
            if (!absFilePath.endsWith(".gz")) {
                absFilePath = compressGZ(new File(absFilePath), false);
                delSrc = true;
            }

            // absFilePath:   /1/10886/xx/d_placement.csv.gz
            // srcRoot:       /1/10886
            // srcRootParent: /1
            // srcRelative:   /10886/xx/d_placement.csv.gz
            Path srcRootParent = new Path(srcRoot).getParent();
            String srcRelative = srcRootParent.toUri().relativize(new Path(absFilePath).toUri()).getPath();
            Path srcCopy = new Path(srcRootParent, srcRelative);
            Path dstCopy = new Path(sinkRoot, srcRelative);
            File gzTablePath = new File(absFilePath);
            long gzSize = gzTablePath.length();
            fs.mkdirs(dstCopy.getParent());
            fs.copyFromLocalFile(delSrc, true, srcCopy, dstCopy);
            long value = cr.filesSize.getOrDefault(gzTablePath.getName(), 0L) + gzSize;
            cr.filesSize.put(gzTablePath.getName(), value);
            cr.fileNum++;
        }

    }

    private String source;
    private String sink;
    private Configuration conf;
    private ExecutorService executor = Executors.newFixedThreadPool(20);
    private ExecutorCompletionService<CallableReport> ecs = new ExecutorCompletionService<CallableReport>(executor);
    private Map<String, Long> tableSize = new HashMap<>();

    public DimensionSync(String processor, Properties props) throws Exception
    {
        this(processor, props, new Configuration());
    }

    public DimensionSync(String processor, Properties props, Configuration configuration) throws Exception
    {
        if (!props.containsKey("source")) {
            throw new RuntimeException("source must be set");
        }
        if (!props.containsKey("sink")) {
            throw new RuntimeException("sink must be set");
        }
        source = props.getProperty("source");
        sink = props.getProperty("sink");
        conf = configuration;
    }

    public void run() throws Exception
    {
        int num = 0;
        File[] files = new File(source).listFiles();
        if (files == null) {
            throw new RuntimeException(String.format("Source error: %s.", source));
        }
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(sink))) {
            System.out.println(String.format("deleting exist sink: %s ...", sink));
            fs.delete(new Path(sink), true);
        }
        for (File dir : files) {
            // assign every network dir a executor
            ecs.submit(new Task(dir.getAbsolutePath(), sink));
            num += 1;
        }

        int successNum = 0;
        System.out.printf("DimensionSync Ready: %d directories.\n", num);
        for (int i = 0; i < num; i++) {
            CallableReport cr = ecs.take().get();
            successNum += cr.fileNum;
            for (String table : cr.filesSize.keySet()) {
                long value = tableSize.getOrDefault(table, 0L);
                value += cr.filesSize.get(table);
                tableSize.put(table, value);
            }
            System.out.printf("Finished: %s.\n", cr.dirName);
        }

        executor.shutdown();
        writeSizeFile();
        System.out.printf("DimensionSync Report: %d files have been sync to hdfs\n", successNum);
    }

    private void writeSizeFile() throws IOException
    {
        FileSystem fs = FileSystem.get(conf);
        Path sizeFile = new Path(sink, "./_SIZE");
        if (fs.exists(sizeFile)) {
            fs.delete(sizeFile, true);
        }
        OutputStreamWriter osw = new OutputStreamWriter(fs.create(sizeFile));
        BufferedWriter bw = new BufferedWriter(osw);
        for (String name : tableSize.keySet()) {
            bw.write(String.format("%s=%d\n", name, tableSize.get(name)));
        }
        bw.close();
    }

    public void cancel() throws Exception
    {
        executor.shutdown();
        System.out.println("DimensionSync has been killed");
    }
}
