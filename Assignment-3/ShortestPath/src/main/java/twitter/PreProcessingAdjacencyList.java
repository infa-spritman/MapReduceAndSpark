package twitter;


import java.io.IOException;
import java.util.ArrayList;

import com.google.common.collect.Iterators;
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

public class PreProcessingAdjacencyList extends Configured implements Tool {
    // Declaring the logger for the program
    private static final Logger logger = LogManager.getLogger(PreProcessingAdjacencyList.class);

    // Mapper for Nodes which emits (x,0) for every row x
    public static class NodesMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final IntWritable followerId = new IntWritable();
        private final Text type = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            followerId.set(Integer.parseInt(value.toString()));
            type.set("NODE");
            context.write(followerId, type);
        }
    }

    // Mapper for Edges which emits (y,1) for every row (x,y)
    public static class EdgesMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final IntWritable followerIdFrom = new IntWritable();
        private final Text tupleTo = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            // Parsing on comma

            final String[] row = value.toString().split(",");

            Integer fromNode = Integer.parseInt(row[0]);
            followerIdFrom.set(fromNode);
            tupleTo.set(row[1]);
            context.write(followerIdFrom,tupleTo);

        }
    }


    // Reducer for the job which reduces the key-value pair produced by Mapper , grouped by key
    public static class ListReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        Text textList = new Text();

        @Override
        public void reduce(final IntWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            // Clear our lists
            textList.clear();
            StringBuffer list = new StringBuffer();


            // iterate through all our values, binning each record based on what
            // it was tagged with
            // make sure to remove the tag!
            list.append("(");
            String prefix = "";
            for (Text t : values) {
                final String row = t.toString();
                if (!row.equals("NODE")) {
                    list.append(prefix);
                    prefix = ":";
                    list.append(row);
                }
            }
            list.append(")");
            textList.set(list.toString());
            context.write(key,textList);

        }
    }

    @Override
    public int run(final String[] args) throws Exception {

        // Initiating configuration for the job
        final Configuration conf = getConf();

        // Defining Job name
        final Job job = Job.getInstance(conf, "Twitter Followers : Adjaceny List");
        job.setJarByClass(PreProcessingAdjacencyList.class);
        final Configuration jobConf = job.getConfiguration();

        // Setting the delimeter for the output
        jobConf.set("mapreduce.output.textoutputformat.separator", "-");

        // Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
        // ================

        job.setMapperClass(EdgesMapper.class);
//        job.setCombinerClass(ListReducer.class);
        job.setReducerClass(ListReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // Adding input path and Nodes Mapper to the job
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, NodesMapper.class);

        // Adding output path and Edges Mapper to the job
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, EdgesMapper.class);

        FileOutputFormat.setOutputPath(job,
                new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(final String[] args) {
        // Checking whether 3 arguments are passed or not
        if (args.length != 3) {
            throw new Error("Four arguments required:\n<input-node-dir> <input-edges-directory> <output-dir>");
        }

        try {
            // Running implementation class
            ToolRunner.run(new PreProcessingAdjacencyList(), args);
        } catch (final Exception e) {
            logger.error("MR Job Exception", e);
        }
    }

}
