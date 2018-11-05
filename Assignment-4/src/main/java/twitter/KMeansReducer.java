package twitter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Random;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    private final Text centroidsOut = new Text();

    @Override
    public void reduce(final IntWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

        Double newCentroidCenter;

        Double sum = 0.0;

        Double num_of_dataPointsInCluster = 0.0;

        Double SSE = 0.0;

        for(Text t: values) {
            // Parsing on comma
            String[] row = t.toString().split(",");

            if(t.toString().contains("DUM")) {
            }
            else {

                // Finding new centroids and calculating SSE
                Integer userId = Integer.parseInt(row[0]);
                Double followerCount = Double.parseDouble(row[1]);
                Double centroidCoordinate = Double.parseDouble(row[2]);

                sum += followerCount;
                SSE += Math.pow(followerCount-centroidCoordinate,2.0);
                num_of_dataPointsInCluster++;
            }
        }
        // Handling Empty cluster
        if(num_of_dataPointsInCluster == 0.0) {
            Random random = new Random();
            newCentroidCenter = random.doubles(0.0, 564512.0).findFirst().getAsDouble();
        } else {
            newCentroidCenter = sum / num_of_dataPointsInCluster;
        }
        long covertedSSE = SSE.longValue();
        // adding SSE to counter
        context.getCounter(KMeansDriver.Convergence.SSE).increment(covertedSSE);
        centroidsOut.set(newCentroidCenter + "," + SSE);
        context.write(key,centroidsOut);
    }

}