package twitter.rs;

import java.io.IOException;
import java.sql.SQLOutput;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class TwitterFollowersReduceSideJoinStep2 extends Configured implements Tool {
    // Declaring the logger for the program
    private static final Logger logger = LogManager.getLogger(TwitterFollowersReduceSideJoinStep2.class);

    // Mapper for Edges which emits (y,1) for every row (x,y)
    public static class TwoPathMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final IntWritable followerIdTo = new IntWritable();
        private final Text tupleTo = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            // Parsing on comma
            final String[] row = value.toString().split(",");

            followerIdTo.set(Integer.parseInt(row[1]));


            tupleTo.set("TO," + row[0]);

            context.write(followerIdTo, tupleTo);

        }
    }

    public static class EdgesMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final IntWritable followerIdFrom = new IntWritable();
        private final Text tupleFrom = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            // Parsing on comma
            final String[] row = value.toString().split(",");

            followerIdFrom.set(Integer.parseInt(row[0]));


            tupleFrom.set("FROM," + row[1]);

            context.write(followerIdFrom, tupleFrom);

        }
    }


    // Reducer for the job which reduces the key-value pair produced by Mapper , grouped by key
    public static class IntSumReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        private final ArrayList<Integer> listLeft = new ArrayList<Integer>();
        private final ArrayList<Integer> listRight = new ArrayList<Integer>();
        private enum TriangleCounter { count };

        @Override
        public void reduce(final IntWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            // Clear our lists
            listLeft.clear();
            listRight.clear();

            // iterate through all our values, binning each record based on what
            // it was tagged with
            // make sure to remove the tag!
            for (Text t : values) {
                final String[] row = t.toString().split(",");
                if (row[0].equals("TO")) {
                    listLeft.add(Integer.parseInt(row[1]));
                } else if (row[0].equals("FROM")) {
                    listRight.add(Integer.parseInt(row[1]));
                }
            }

            // Execute our join logic now that the lists are filled
            if (!listLeft.isEmpty() && !listRight.isEmpty()) {
                for (Integer left : listLeft) {
                    for (Integer right : listRight) {
                        if(left.equals(right))
                            context.getCounter(TriangleCounter.count).increment(1);
                    }
                }
            }

        }
    }

    @Override
    public int run(final String[] args) throws Exception {

        // Initiating configuration for the job
        final Configuration conf = getConf();

        // Defining Job name
        final Job job = Job.getInstance(conf, "Twitter Followers : Reduce Side Join Step 2");
        job.setJarByClass(TwitterFollowersReduceSideJoinStep2.class);
        final Configuration jobConf = job.getConfiguration();

        // Setting the delimeter for the output
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        // Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
        // ================

        // Adding input path and TwoPath Mapper Mapper to the job
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TwoPathMapper.class);

        // Adding output path and Edges Mapper to the job
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, EdgesMapper.class);

        // Setting the reducer for the job
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Setting the output key and value type
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        int result = job.waitForCompletion(true) ? 0 : 1;

        Counter triangleCount =job.getCounters().findCounter(IntSumReducer.TriangleCounter.count);

        System.out.println(triangleCount.getDisplayName()+ " : " + triangleCount.getValue()/3);

        return result;

    }

    public static void main(final String[] args) {
        // Checking whether 3 arguments are passed or not
        if (args.length != 3) {
            throw new Error("Two arguments required:\n<input-node-dir> <input-edges-directory> <output-dir>");
        }

        try {
            // Running implementation class
            ToolRunner.run(new TwitterFollowersReduceSideJoinStep2(), args);
        } catch (final Exception e) {
            logger.error("MR Job Exception", e);
        }
    }

}