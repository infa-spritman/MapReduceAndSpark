package twitter;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class Cardinality extends Configured implements Tool {
    // Declaring the logger for the program
    private static final Logger logger = LogManager.getLogger(Cardinality.class);

    public static void main(final String[] args) {
        // Checking whether 3 arguments are passed or not
        if (args.length != 4) {
            throw new Error("Four arguments required:\n<input-node-dir> <input-edges-directory> <output-dir> <MAX-Value>");
        }

        try {
            // Running implementation class
            ToolRunner.run(new Cardinality(), args);
        } catch (final Exception e) {
            logger.error("MR Job Exception", e);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {

        // Initiating configuration for the job
        final Configuration conf = getConf();

        // Defining Job name
        final Job job = Job.getInstance(conf, "Twitter Followers : Reduce Side Join Step 1");
        job.setJarByClass(Cardinality.class);
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
//        job.setCombinerClass(ListReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job,
                new Path(args[2]));
        int result = job.waitForCompletion(true) ? 0 : 3;

        Counter cardinalityCount = job.getCounters().findCounter(IntSumReducer.cardinality.count);

        System.out.println(cardinalityCount.getDisplayName() + " : " + cardinalityCount.getValue());

        return result;

    }

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

            Integer toNode = Integer.parseInt(row[1]);
            Integer fromNode = Integer.parseInt(row[0]);


            followerIdTo.set(toNode);
            followerIdFrom.set(fromNode);

            tupleTo.set("TO");
            tupleFrom.set("FROM");

            context.write(followerIdTo, tupleTo);
            context.write(followerIdFrom, tupleFrom);


        }
    }

    // Reducer for the job which reduces the key-value pair produced by Mapper , grouped by key
    public static class IntSumReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

        @Override
        public void reduce(final IntWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            // Clear our lists
            long inDegree = 0L;
            long outdegree = 0L;

            // iterate through all our values, binning each record based on what
            // it was tagged with
            // make sure to remove the tag!
            for (Text t : values) {
                final String row = t.toString();
                if (row.equals("TO")) {
                    inDegree++;
                } else if (row.equals("FROM")) {
                    outdegree++;
                }
            }

            context.getCounter(cardinality.count).increment(inDegree * outdegree);
        }

        private enum cardinality {count}
    }

}