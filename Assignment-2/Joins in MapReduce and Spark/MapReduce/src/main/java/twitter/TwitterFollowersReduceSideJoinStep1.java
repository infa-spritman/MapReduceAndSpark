package twitter;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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

public class TwitterFollowersReduceSideJoinStep1 extends Configured implements Tool {
    // Declaring the logger for the program
    private static final Logger logger = LogManager.getLogger(TwitterFollowersReduceSideJoinStep1.class);

    // Mapper for Edges which emits (y,1) for every row (x,y)
    public static class EdgesMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final IntWritable followerIdTo = new IntWritable();
        private final IntWritable followerIdFrom = new IntWritable();
        private final Text tupleTo = new Text();
        private final Text tupleFrom = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            // Parsing on comma
            final String[] row = value.toString().split(",");

            followerIdTo.set(Integer.parseInt(row[1]));
            followerIdFrom.set(Integer.parseInt(row[0]));

            tupleTo.set("TO," + row[0] + "," + row[1]);
            tupleFrom.set("FROM," + row[0] + "," + row[1]);

            context.write(followerIdTo, tupleTo);
            context.write(followerIdFrom, tupleFrom);

        }
    }


    // Reducer for the job which reduces the key-value pair produced by Mapper , grouped by key
    public static class IntSumReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        private ArrayList<IntWritable> listLeft = new ArrayList<IntWritable>();
        private ArrayList<IntWritable> listRight = new ArrayList<IntWritable>();

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
                    listLeft.add(new IntWritable(Integer.parseInt(row[1])));
                } else if (row[0].equals("FROM")) {
                    listRight.add(new IntWritable(Integer.parseInt(row[2])));
                }
            }

            // Execute our join logic now that the lists are filled
            if (!listLeft.isEmpty() && !listRight.isEmpty()) {
                for (IntWritable left : listLeft) {
                    for (IntWritable right : listRight) {
                        context.write(left, right);
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
        final Job job = Job.getInstance(conf, "Twitter Followers : Reduce Side Join Step 1");
        job.setJarByClass(TwitterFollowersReduceSideJoinStep1.class);
        final Configuration jobConf = job.getConfiguration();

        // Setting the delimeter for the output
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        // Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
        // ================

        job.setMapperClass(EdgesMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job,
                new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(final String[] args) {
        // Checking whether 3 arguments are passed or not
        if (args.length != 3) {
            throw new Error("Two arguments required:\n<input-node-dir> <input-edges-directory> <output-dir>");
        }

        try {
            // Running implementation class
            ToolRunner.run(new TwitterFollowersReduceSideJoinStep1(), args);
        } catch (final Exception e) {
            logger.error("MR Job Exception", e);
        }
    }

}