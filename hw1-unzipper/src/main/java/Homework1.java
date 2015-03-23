import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.*;
import java.util.*;


public class Homework1 {

    public static final int NUM_REDUCERS = 50;


    /* Split representing a folder name */
    public static class UniqueFolderSplit extends InputSplit implements Writable  {

        private Path folder;

        UniqueFolderSplit(){

        }

        private UniqueFolderSplit(Path folder) {
            this.folder = folder;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            String[] rv = {folder.toString()};
            return new String[0];
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            new Text(folder.toString()).write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            Text read = new Text();
            read.readFields(dataInput);
            this.folder = new Path(read.toString());
        }
    }

    /* Represents the input format that reads a folder and returns the list of files along with their lengths */
    public static class FolderInputFormat extends InputFormat<Text, Text> {

        /**
         * Copied from FileInputFormat
         */
        public static void addInputPath(Job job, Path path) throws IOException {
            Configuration conf = job.getConfiguration();
            path = path.getFileSystem(conf).makeQualified(path);
            String dirStr = StringUtils.escapeString(path.toString());
            String dirs = conf.get("mapreduce.input.folderinputformat.inputdir");
            conf.set("mapreduce.input.folderinputformat.inputdir", dirs == null ? dirStr : dirs + "," + dirStr);
        }

        /**
         * Copied from FileInputFormat
         */
        public static Path[] getInputPaths(JobContext context) {
            String dirs = context.getConfiguration().get("mapreduce.input.folderinputformat.inputdir", "");
            String[] list = StringUtils.split(dirs);
            Path[] result = new Path[list.length];

            for (int i = 0; i < list.length; ++i) {
                result[i] = new Path(StringUtils.unEscapeString(list[i]));
            }

            return result;
        }

        @Override
        public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
            Path[] folders = getInputPaths(jobContext);
            List<InputSplit> folderSplits = new ArrayList<>(folders.length);
            for (Path folder : folders) {
                InputSplit split = new UniqueFolderSplit(folder);
                folderSplits.add(split);
            }

            return folderSplits;
        }

        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            UniqueFolderSplit split = (UniqueFolderSplit) inputSplit;
            RecordReader<Text, Text> recordReader = new FolderRecordReader();
            recordReader.initialize(inputSplit, taskAttemptContext);
            return recordReader;
        }
    }

    public static class FolderRecordReader extends RecordReader<Text, Text> {
        private boolean read = false;
        private UniqueFolderSplit split = null;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            this.split = (UniqueFolderSplit) inputSplit;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            boolean wasRead = read;
            read = true;
            return !wasRead;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return new Text(split.folder.toString());
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return new Text(split.folder.toString());
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return read ? 100 : 0;
        }

        @Override
        public void close() throws IOException {

        }
    }

    /**
     * For every folder that has files to be unzipped, the mapper will get a list of files
     * and their size and perform load balancing. Concretely, it will assign an id to each
     * file such that each id has approximately the same number of bytes to process.
     */
    public static class LoadBalanceMapper extends Mapper<Text, Text, LongWritable, Text> {
        private final static LongWritable reducerId = new LongWritable();
        private Text inputFile = new Text();

        @Override
        public void map(Text key, Text folder, Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());

            FileStatus[] filesStatuses = fs.listStatus(new Path(folder.toString()), new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return path.getName().endsWith(".gz");
                }
            });

            /** Sort files according to size, biggest first */
            SortedSet<FileToUnzip> files = new TreeSet<FileToUnzip>();
            for (FileStatus fileStatus : filesStatuses) {
                files.add(new FileToUnzip(fileStatus.getLen(), fileStatus.getPath().toString()));
            }

            /* Create representation of reducers */
            PriorityQueue<ReducerLoad> loads = new PriorityQueue<>();
            for (long i = 0; i < NUM_REDUCERS; ++i) {
                loads.add(new ReducerLoad(i));
            }


            for (FileToUnzip file : files) {
                ReducerLoad load = loads.poll(); /* Get reducer with smallest load yet */
                load.sendBytes(file.length); /* Add current file */

                reducerId.set(load.getId()); /* Retrieve reducer id */
                inputFile.set(file.inputFile);

                loads.add(load); /* Return load to queue */

                context.write(reducerId, inputFile);
            }

        }
    }

    /**
     * Simple reducer
     */
    public static class GunzipReducer extends Reducer<LongWritable, Text, Text, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            Path outputFolder = FileOutputFormat.getOutputPath(context);

            FileSystem fs = FileSystem.get(conf);
            CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
            CompressionCodec codec = ccf.getCodecByClassName(GzipCodec.class.getName());

            for (Text value : values) {
                /* Build path for input file */
                Path inputFile = new Path(value.toString());

                /* Build path for output file */
                String filename = inputFile.getName();
                Path outputFile = new Path(outputFolder, filename.substring(0, filename.lastIndexOf(".gz")));

                /* Copy bytes */
                InputStream is = fs.open(inputFile);
                OutputStream os = fs.create(outputFile);

                CompressionInputStream cis = codec.createInputStream(is);
                IOUtils.copyBytes(cis, os, conf);

                is.close();
                os.close();

                context.write(new Text(inputFile.toString()), new Text(outputFile.toString()));
            }
        }
    }

    /**
     * Represents a file we want to unzip
     */
    static class FileToUnzip implements Comparable<FileToUnzip> {
        private Long length;
        private String inputFile;

        FileToUnzip(long length, String inputFile) {
            this.length = length;
            this.inputFile = inputFile;
        }

        @Override
        public int compareTo(FileToUnzip o) {
            /* Reverse order, because we want them from bigger to smaller */
            return o.length.compareTo(this.length);
        }
    }

    /**
     * Represents the load we decided to send to one Reducer
     */
    static class ReducerLoad implements Comparable<ReducerLoad> {
        private Long bytesToProcess;
        private Long id;

        ReducerLoad(long id) {
            this.bytesToProcess = 0L;
            this.id = id;
        }

        public void sendBytes(long numBytes) {
            bytesToProcess += numBytes;
        }

        public Long getId() {
            return id;
        }

        @Override
        public int compareTo(ReducerLoad o) {
            return this.bytesToProcess.compareTo(o.bytesToProcess);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Homework1");

        job.setJarByClass(Homework1.class);

        job.setMapperClass(LoadBalanceMapper.class);
        job.setReducerClass(GunzipReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(NUM_REDUCERS);


        Path inputFolder = new Path(args[0]);
        Path outputFolder = new Path(args[1]);

        job.setInputFormatClass(FolderInputFormat.class);
        FolderInputFormat.addInputPath(job, inputFolder);
        job.setOutputFormatClass(NullOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputFolder);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

