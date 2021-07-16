package pkg;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class TwitterExtractMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String jsonString = value.toString();
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
        if (jsonObject.has("created_at")) {
            String id_str = jsonObject.get("id").toString();
            String created_at = jsonObject.get("created_at").toString();
            String text = jsonObject.get("text").toString();
            String id1 = jsonObject.get("id").toString();
            int id = Integer.parseInt(id1);
            String lang = jsonObject.get("lang").toString();
            if (lang.equals("en")) {
                String v = text + "/t" + lang;
                context.write(new Text("id: " + id_str + "\t" + "created_at: "), new Text(v));
            }
        }
    }
}
