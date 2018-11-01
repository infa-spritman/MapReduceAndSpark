package twitter;

import com.google.gson.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ShortestPathMapper extends Mapper<Object, Text, IntWritable, JsonWritable> {
    private final IntWritable outIntWritable = new IntWritable();
    private final JsonParser PARSER = new JsonParser();

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

        Configuration configuration = context.getConfiguration();
        int currentCounter = configuration.getInt("counter", -1);
        final List<String> sources = Arrays.stream(configuration.get("sources").split(",")).collect(Collectors.toList());
        // Parsing on comma
        final String[] row = value.toString().split("-");
        Integer from = Integer.parseInt(row[0]);
        JsonWritable fromVertex = new JsonWritable();
        JsonWritable fromVertexReducer = new JsonWritable();
        Counter diameter = context.getCounter(ShotestPathDriver.Diameter.path);
        diameter.setValue(currentCounter);


        if (currentCounter == 1) {
            String listWithoutBrackets = row[1].substring(1, row[1].length() - 1);
            JsonObject finalJsonVertexObject = (JsonObject) PARSER.parse("{ id:" + row[0] + "}");
            JsonArray adjacencyList = new JsonArray();
            JsonObject hashMapJsonObect = new JsonObject();
            for (String src : sources) {
                if (src.equals(row[0]))
                    hashMapJsonObect.addProperty(src, 0);
                else
                    hashMapJsonObect.addProperty(src, Integer.MAX_VALUE);

            }
            if (listWithoutBrackets.length() != 0) {
                for (String to : listWithoutBrackets.split(":")) {
                    adjacencyList.add(new JsonPrimitive(to));
                }
            }


            finalJsonVertexObject.add("sourcesDistance", hashMapJsonObect);
            finalJsonVertexObject.addProperty("isVertex", true);
            finalJsonVertexObject.add("adjacencyList", adjacencyList);
            fromVertex.set(finalJsonVertexObject);

        } else {
            fromVertex.set(row[1]);
        }

        context.write(new IntWritable(from), fromVertex);


        /////////////////////////////
        //////////////////////////////
        ////////////////////////////

        if (currentCounter == 1) {
            String listWithoutBrackets = row[1].substring(1, row[1].length() - 1);

            if(!listWithoutBrackets.equals("")) {

                for (String to : listWithoutBrackets.split(":")) {

                    outIntWritable.set(Integer.parseInt(to));
                    JsonObject finalJsonVertexObject = (JsonObject) PARSER.parse("{ id:" + to + "}");
                    JsonObject hashMapJsonObect = new JsonObject();
                    for (String src : sources) {
                        if (src.equals(row[0]))
                            hashMapJsonObect.addProperty(src, 1);
                        else
                            hashMapJsonObect.addProperty(src, Integer.MAX_VALUE);

                    }

                    finalJsonVertexObject.add("sourcesDistance", hashMapJsonObect);
                    finalJsonVertexObject.addProperty("isVertex", false);
                    fromVertex.set(finalJsonVertexObject);
                    context.write(outIntWritable, fromVertex);

                }
            }
        } else {
            JsonWritable existingJson = new JsonWritable(row[1]);
            JsonObject jsonObject = existingJson.getJsonObject();
            JsonObject sourcesDistanceTillCurrentVertex = jsonObject.getAsJsonObject("sourcesDistance");
            JsonArray adjacencyList = jsonObject.getAsJsonArray("adjacencyList");

            if (adjacencyList.size() !=0){
                for (JsonElement je : adjacencyList) {
                    Integer m = je.getAsInt();
                    outIntWritable.set(m);
                    JsonObject finalJsonVertexObject = (JsonObject) PARSER.parse("{ id:" + m + "}");
                    JsonObject hashMapJsonObect = new JsonObject();
                    for (String src : sources) {
                        int d = sourcesDistanceTillCurrentVertex.get(src).getAsInt();
                        if (d == Integer.MAX_VALUE)
                            hashMapJsonObect.addProperty(src, Integer.MAX_VALUE);
                        else
                            hashMapJsonObect.addProperty(src, d + 1);

                    }

                    finalJsonVertexObject.add("sourcesDistance", hashMapJsonObect);
                    finalJsonVertexObject.addProperty("isVertex", false);
                    fromVertex.set(finalJsonVertexObject);
                    context.write(outIntWritable, fromVertex);


                }
            }

        }
    }
}