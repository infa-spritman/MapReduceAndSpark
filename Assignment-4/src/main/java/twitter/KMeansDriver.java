package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.net.URI;


public class KMeansDriver extends Configured implements Tool {

    // Declaring the logger for the program
    private static final Logger logger = LogManager.getLogger(KMeansDriver.class);

    public static void main(final String[] args) {
        // Checking whether 4 arguments are passed or not
        if (args.length != 3) {
            throw new Error("Four arguments required:\n<centroid-dir> <output-path> k <datapoints-input-path>");
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

        double previousSSE;
        double currentSSE;

        double differenceSSE = 1.0;

        int completed = 0;


        while (differenceSSE > 0 && iterationCounter <=10) {


            logger.info("Iteration Counter: " + iterationCounter);

            String centroidInputPath, outputPath;

            if (iterationCounter == 1) {
                centroidInputPath = args[0];
            } else {
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
            job.setJarByClass(KMeansDriver.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(DoubleWritable.class);


            FileInputFormat.addInputPath(job, new Path(args[3]));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            previousSSE = job.getCounters().findCounter(Convergence.SSE).getValue() / 10000.0;

            // Configure the DistributedCache

            Path centroidPath = new Path(centroidInputPath);

            FileSystem fs = FileSystem.get(centroidPath.toUri(), conf);

            RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(centroidPath, false);

            while (locatedFileStatusRemoteIterator.hasNext()) {
                URI uri = locatedFileStatusRemoteIterator.next().getPath().toUri();
                job.addCacheFile(uri);

            }

            completed = job.waitForCompletion(true) ? 0 : 1;

            currentSSE = job.getCounters().findCounter(Convergence.SSE).getValue() / 10000.0;

            differenceSSE = currentSSE - previousSSE;

            iterationCounter += 1;
        }
        return completed;
    }

    enum Convergence {
        SSE
    }

}
