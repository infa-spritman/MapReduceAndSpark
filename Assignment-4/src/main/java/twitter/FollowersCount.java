package twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class FollowersCount extends Configured implements Tool {
    // Declaring the logger for the program
    private static final Logger logger = LogManager.getLogger(FollowersCount.class);

    public static void main(final String[] args) {
        // Checking whether 3 arguments are passed or not
        if (args.length != 3) {
            throw new Error("Four arguments required:\n<input-node-dir> <input-edges-directory> <output-dir>");
        }

        try {
            // Running implementation class
            ToolRunner.run(new FollowersCount(), args);
        } catch (final Exception e) {
            logger.error("MR Job Exception", e);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {

        // Initiating configuration for the job
        final Configuration conf = getConf();

        // Defining Job name
        final Job job = Job.getInstance(conf, "Twitter Followers : Count");
        job.setJarByClass(FollowersCount.class);
        final Configuration jobConf = job.getConfiguration();

        // Setting the delimeter for the output
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        // Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
        // ================

        // Adding input path and Nodes Mapper to the job
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, NodesMapper.class);

        // Adding output path and Edges Mapper to the job
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, EdgesMapper.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);


        FileOutputFormat.setOutputPath(job,
                new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;

    }


    // Mapper for Nodes which emits (x,0) for every row x
    public static class NodesMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        private final IntWritable followerId = new IntWritable();
        private final DoubleWritable count = new DoubleWritable();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            followerId.set(Integer.parseInt(value.toString()));
            count.set(0.0);
            context.write(followerId, count);
        }
    }

    // Mapper for Edges which emits (y,1) for every row (x,y)
    public static class EdgesMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        private final IntWritable followerIdTo = new IntWritable();
        private final DoubleWritable count = new DoubleWritable();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            // Parsing on comma

            final String[] row = value.toString().split(",");

            Integer toNode = Integer.parseInt(row[1]);

            followerIdTo.set(toNode);

            count.set(1.0);

            context.write(followerIdTo, count);


        }
    }

    // Reducer for the job which reduces the key-value pair produced by Mapper , grouped by key
    public static class IntSumReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        DoubleWritable finalCount = new DoubleWritable();
        @Override
        public void reduce(final IntWritable key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException {
            // Clear our lists
            Double totalCount = 0.0;

            // iterate through all our values, add all the values
            for (DoubleWritable t : values) {
                totalCount += t.get();
            }

            finalCount.set(totalCount);

            context.write(key, finalCount);

        }

    }

}