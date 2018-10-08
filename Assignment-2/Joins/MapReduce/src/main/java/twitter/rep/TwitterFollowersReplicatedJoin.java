package twitter.rep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TwitterFollowersReplicatedJoin extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(TwitterFollowersReplicatedJoin.class);

    public static void main(final String[] args) {
        // Checking whether 3 arguments are passed or not
        if (args.length != 4) {
            throw new Error("Four arguments required:\n<input-node-dir> <input-edges-directory> <output-dir> <MAX-Value>");
        }

        try {
            // Running implementation class
            ToolRunner.run(new TwitterFollowersReplicatedJoin(), args);
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
        job.setJarByClass(TwitterFollowersReplicatedJoin.class);
        final Configuration jobConf = job.getConfiguration();

        // Setting the delimeter for the output
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        jobConf.setInt("MAX", Integer.parseInt(args[3]));

        // Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
        // ================

        job.setMapperClass(ReplicatedJoinMapper.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job,
                new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Configure the DistributedCache

        Path path = new Path(args[1]);

        FileSystem fs = FileSystem.get(path.toUri(), conf);

        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(path, false);

        while (locatedFileStatusRemoteIterator.hasNext()) {
            URI uri = locatedFileStatusRemoteIterator.next().getPath().toUri();
            job.addCacheFile(uri);

        }

        int result = job.waitForCompletion(true) ? 0 : 3;

        Counter triangleCount = job.getCounters().findCounter(ReplicatedJoinMapper.TriangleCounter.count);

        System.out.println(triangleCount.getDisplayName() + " : " + triangleCount.getValue() / 3);

        return result;

    }

    public static class ReplicatedJoinMapper extends
            Mapper<Object, Text, Text, Text> {

        private HashMap<Integer, List<Integer>> adjacencyList = new HashMap<Integer, List<Integer>>();

        private enum TriangleCounter {count}

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            try {
                URI[] files = context.getCacheFiles();

                if (files == null || files.length == 0) {
                    throw new RuntimeException(
                            "User information is not set in DistributedCache");
                }

                // Read all files in the DistributedCache
                for (URI p : files) {
//                    System.out.println("Path:" + p);
                    FileSystem fs = FileSystem.get(p, context.getConfiguration());

                    BufferedReader rdr = new BufferedReader(
                            new InputStreamReader(fs.open(new Path(p))));

                    String line;
                    // For each record in the user file
                    while ((line = rdr.readLine()) != null) {

                        String[] split = line.split(",");

                        if (split.length != 0) {
                            // Map the user ID to the record
                            Integer fromID = Integer.parseInt(split[0]);
                            Integer toID = Integer.parseInt(split[1]);
                            Integer MAX = context.getConfiguration().getInt("MAX", Integer.MAX_VALUE);

                            if (toID < MAX && fromID < MAX) {
                                if (adjacencyList.containsKey(fromID)) {
                                    List<Integer> tempList = adjacencyList.get(fromID);
                                    tempList.add(toID);
                                    adjacencyList.put(fromID, tempList);
                                } else {
                                    List<Integer> integerList = new ArrayList<>();
                                    integerList.add(toID);
                                    adjacencyList.put(fromID, integerList);
                                }
                            }
                        }
                    }
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        ;


//        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            final String[] row = value.toString().split(",");

            Integer toNode = Integer.parseInt(row[1]);
            Integer fromNode = Integer.parseInt(row[0]);
            Integer MAX = context.getConfiguration().getInt("MAX", -1);

            if (toNode < MAX && fromNode < MAX) {
                if (adjacencyList.containsKey(toNode)) {
                    List<Integer> midNodeOutEdges = adjacencyList.get(toNode);
                    if (midNodeOutEdges != null && !midNodeOutEdges.isEmpty()) {
                        for (Integer nodeOut : midNodeOutEdges) {
                            List<Integer> nodeOutEdges = adjacencyList.get(nodeOut);
                            if (nodeOutEdges != null && !nodeOutEdges.isEmpty() && nodeOutEdges.contains(fromNode))
                                context.getCounter(TriangleCounter.count).increment(1);
                        }
                    }
                }
            }
        }


    }

}
