package pkg;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NounExtractMapper extends Mapper<LongWritable, Text, Text, Text>{
	String token = "";
	String proper_token = "";
	String adjective = "";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String file = value.toString().trim();
		String[] sentences = file.split("\t");
		if(sentences.length == 2){
			String tagged = sentences[0];
			String tweet = sentences[1];
			context.write(new Text(tweet), new Text(tagged));
		}
	}
}
