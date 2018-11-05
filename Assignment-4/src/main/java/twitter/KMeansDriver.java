package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.net.URI;
import java.util.Random;


public class KMeansDriver extends Configured implements Tool {

    // Declaring the logger for the program
    private static final Logger logger = LogManager.getLogger(KMeansDriver.class);

    public static void main(final String[] args) {
        // Checking whether 4 arguments are passed or not
        if (args.length != 4) {
            throw new Error("Three arguments required:\n<input-path> <output-path> k start-config");
        }

        try {
            // Running implementation class
            ToolRunner.run(new KMeansDriver(), args);
        } catch (final Exception e) {
            logger.error("MR Job Exception", e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();

        int iterationCounter = 1;

        double previousSSE = 0.0;
        double currentSSE;
        double differenceSSE = 3.0;

        int completed = 0;

        Double maxFollowerCount = 564512.0;
        Integer K = Integer.parseInt(args[2]);
        String config = args[3];
        int size = (int) Math.ceil(maxFollowerCount / K.doubleValue());
        StringBuilder intialCentroidString = new StringBuilder();
        Random random = new Random();

        if (config.equals("GS1")) {

            for (int i = 0; i < K; i++) {

                intialCentroidString.append(random.doubles(Double.valueOf(i * size), Double.valueOf((i + 1) * size)).findFirst().getAsDouble() + ",");
            }

        } else if (config.equals("GS2")) {

        } else if (config.equals("BS")) {

            final double randomCentroid = random.doubles(0, maxFollowerCount - K).findFirst().getAsDouble();

            for (int i = 0; i < K; i++) {

                double temp = randomCentroid + (double) i;
                intialCentroidString.append(temp + ",");

            }

        }

        conf.set("initial-centroids", intialCentroidString.substring(0, intialCentroidString.length() - 1));
        logger.info("intial-cen" + intialCentroidString.toString());

        while (differenceSSE > 2.0) {


            logger.info("Iteration Counter: " + iterationCounter);

            String centroidInputPath = null, outputPath;

            if (iterationCounter != 1) {
                centroidInputPath = args[1] + "-" + (iterationCounter - 1);
            }
            outputPath = args[1] + "-" + iterationCounter;

            // Defining Job name
            final Job job = Job.getInstance(conf, "Twitter Followers : K Means");
            job.setJarByClass(KMeansDriver.class);
            final Configuration jobConf = job.getConfiguration();

            // Setting the delimeter for the output
            jobConf.set("mapreduce.output.textoutputformat.separator", ",");
            jobConf.setInt("counter", iterationCounter);
            jobConf.setInt("K", Integer.parseInt(args[2]));

            // Setting the classes
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
//            previousSSE = job.getCounters().findCounter(Convergence.SSE).getValue() / 10000.0;

            // Configure the DistributedCache
            if (iterationCounter != 1) {
                Path centroidPath = new Path(centroidInputPath);

                FileSystem fs = FileSystem.get(centroidPath.toUri(), conf);

                RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(centroidPath, false);

                while (locatedFileStatusRemoteIterator.hasNext()) {
                    URI uri = locatedFileStatusRemoteIterator.next().getPath().toUri();
                    job.addCacheFile(uri);

                }
            }
            completed = job.waitForCompletion(true) ? 0 : 1;

            currentSSE = job.getCounters().findCounter(Convergence.SSE).getValue();

            differenceSSE = Math.abs(currentSSE - previousSSE);

            previousSSE = currentSSE;

            logger.info("Iteration-count :" + iterationCounter + ", Difference:" + differenceSSE);
            logger.info("Iteration-count :" + iterationCounter + ", SSE:" + currentSSE);

            iterationCounter += 1;
        }
        return completed;
    }

    enum Convergence {
        SSE
    }

}
