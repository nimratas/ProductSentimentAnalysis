package pkg;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PhoneExtractMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String file = value.toString();
		String[] parsed = file.split("\t");
		String tweet = parsed[0];
		String nouns = parsed[1];
		
		context.write(new Text(tweet), new Text(nouns));
	}

}
