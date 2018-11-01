package twitter;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ShortestPathReducer extends Reducer<IntWritable, JsonWritable, IntWritable, Text> {

    private final JsonParser PARSER = new JsonParser();


    @Override
    public void reduce(final IntWritable key, final Iterable<JsonWritable> values, final Context context) throws IOException, InterruptedException {


        // iterate through all our values, binning each record based on what
        // it was tagged with
        // make sure to remove the tag!
        Text result = new Text();
        JsonObject resultObject = (JsonObject) PARSER.parse("{ id:" + key.get() + "}");
        JsonObject dminJsonObjec = new JsonObject();
        JsonObject dexistingJsonObject = new JsonObject();
        Configuration configuration = context.getConfiguration();

        final List<String> sources = Arrays.stream(configuration.get("sources").split(",")).collect(Collectors.toList());
        int currentCounter = configuration.getInt("counter", -1);

        for(String src : sources) {
            dminJsonObjec.addProperty(src, Integer.MAX_VALUE);
            dexistingJsonObject.addProperty(src, Integer.MAX_VALUE);
        }

        JsonArray adjacencyList = new JsonArray();


        Counter updated = context.getCounter(ShotestPathDriver.Convergence.numUpdated);



        for (JsonWritable js : values) {
            JsonObject currentJsonObject = js.getJsonObject();
            boolean isVertex = currentJsonObject.get("isVertex").getAsBoolean();
            if (isVertex) {
                dexistingJsonObject = currentJsonObject.getAsJsonObject("sourcesDistance");
                for(String src : sources){
                    if(dexistingJsonObject.get(src).getAsInt() < dminJsonObjec.get(src).getAsInt()){
                        dminJsonObjec.addProperty(src, dexistingJsonObject.get(src).getAsInt());
                    }
                }
                adjacencyList = currentJsonObject.getAsJsonArray("adjacencyList");
            } else {
                JsonObject distance = currentJsonObject.getAsJsonObject("sourcesDistance");
                for(String src : sources){
                    if(distance.get(src).getAsInt() < dminJsonObjec.get(src).getAsInt()){
                        dminJsonObjec.addProperty(src, distance.get(src).getAsInt());
                    }
                }
            }
        }

        for(String src : sources){
            if(dminJsonObjec.get(src).getAsInt() < dexistingJsonObject.get(src).getAsInt()){
                updated.increment(1);
            }
        }


        resultObject.add("sourcesDistance", dminJsonObjec);
        resultObject.addProperty("isVertex", true);
        resultObject.add("adjacencyList", adjacencyList);
        result.set(resultObject.toString());

        context.write(key,result);


        //emit
    }

}