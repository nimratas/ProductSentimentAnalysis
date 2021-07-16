package pkg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CategoryExtractReducer extends Reducer<Text, Text,Text, Text> {
	public static Map<String,String> matches = new HashMap<String, String>();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

		ArrayList<String[]> phrase = new ArrayList<String[]>();// for nouns/propernouns/adjectives
		ArrayList<String> product = new ArrayList<String>();// product name
		String line = "";
		String output = "";
		String camera = "camera";

		if(product.isEmpty()){
			// I want to put all the product names in this data structure from categories.tar
			Path path = new Path(context.getConfiguration().get("CAMERA_RATING"));
			FileSystem file = FileSystem.get(new Configuration());
			BufferedReader reader = new BufferedReader(new InputStreamReader (file.open(path)));
			while((line = reader.readLine())!= null){
				String lower = line.toLowerCase().trim();
				if (lower.contains(camera)){
					lower = lower.replaceAll("camera", "NA");
				}
				product.add(lower);
			}
			reader.close();
		}
		for (Text val : values) {
			String np = val.toString().toLowerCase();
			if (np.length() >= 1) {
				String[] broken = np.split(" ");
				// add noun/propernoun/adjective string to the list
				phrase.add(broken);
			}
		}

		for(String sentence: product){
			int flag = 0;
			for(String[] elementArray : phrase ){
				if(flag == 0){
					for(String word: elementArray){
						word = word.trim();
						if ((word.length() > 2) && (word != "!!")) {
							word = (" " + word + " ");
							if (sentence.contains(word) && (word.length() > 2)) {
								System.out.println("***word***" + word);
								System.out.println("***sentence***" + sentence);
								flag = 1;
							}
						} else {
							flag = 0;
						}
					}
				}

			}
			if (flag == 1) {
				context.write(key, new Text(sentence));
			}

		}

	}
}
