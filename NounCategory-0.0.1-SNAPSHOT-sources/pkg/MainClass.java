package pkg;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainClass {
	public static final String OUTPUT_PATH1 = "noun_output";
	public static final String OUTPUT_PATH2 = "camera_rating";
	public static final String OUTPUT_PATH3 = "phone_rating_output";
	public static final String OUTPUT_PATH4 = "laptop_rating_output";
	public static final String OUTPUT_PATH5 = "TV_rating_output";
	public static final String OUTPUT_PATH6 = "Tablet_rating_output";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		if(args.length != 1){
			System.out.println("Insufficient Input");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(MainClass.class);
		job.setMapperClass(NounExtractMapper.class);
		job.setReducerClass(NounExtractReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputDirRecursive(job,true);
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH1));
		if (job.waitForCompletion(true)) {
			System.out.println("JOB ONE COMPLETION!!");
		}

		conf.set("PHONE_RATING", "hdfs://lincoln:30381" + "/Testruns/mobile_rating.txt" ); 

		Job phoneCat = Job.getInstance(conf);
		phoneCat.setJarByClass(MainClass.class);
		phoneCat.setMapperClass(PhoneExtractMapper.class);
		phoneCat.setReducerClass(PhoneExtractReducer.class);
		phoneCat.setOutputKeyClass(Text.class);
		phoneCat.setOutputValueClass(Text.class);
		
		phoneCat.setInputFormatClass(TextInputFormat.class);
		phoneCat.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputDirRecursive(phoneCat, true);
		FileInputFormat.setInputPaths(phoneCat, new Path(OUTPUT_PATH1));//phone rating file 
		FileOutputFormat.setOutputPath(phoneCat, new Path(OUTPUT_PATH3));
		if (phoneCat.waitForCompletion(true)) {
			System.out.println("JOB THREE COMPLETION!!");
		}
		
		conf.set("LAPTOP_RATING", "hdfs://lincoln:30381" + "/Testruns/laptop_rating" ); 

		Job laptopCat = Job.getInstance(conf);
		laptopCat.setJarByClass(MainClass.class);
		laptopCat.setMapperClass(LaptopExtractMapper.class);
		laptopCat.setReducerClass(LaptopExtractReducer.class);
		laptopCat.setOutputKeyClass(Text.class);
		laptopCat.setOutputValueClass(Text.class);
		laptopCat.setInputFormatClass(TextInputFormat.class);
		laptopCat.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputDirRecursive(laptopCat, true);
		FileInputFormat.setInputPaths(laptopCat, new Path(OUTPUT_PATH1));//laptop rating file 
		FileOutputFormat.setOutputPath(laptopCat, new Path(OUTPUT_PATH4));
		if (laptopCat.waitForCompletion(true)) {
			System.out.println("JOB FOUR COMPLETION!!");
		}
		
		conf.set("TV_RATING", "hdfs://lincoln:30381" + "/Testruns/tv_rating" ); 

		Job tvCat = Job.getInstance(conf);
		tvCat.setJarByClass(MainClass.class);
		tvCat.setMapperClass(TVExtractMapper.class);
		tvCat.setReducerClass(TVExtractReducer.class);
		tvCat.setOutputKeyClass(Text.class);
		tvCat.setOutputValueClass(Text.class);
		tvCat.setInputFormatClass(TextInputFormat.class);
		tvCat.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputDirRecursive(tvCat, true);

		FileInputFormat.setInputPaths(tvCat, new Path(OUTPUT_PATH1));//tv rating file 
		FileOutputFormat.setOutputPath(tvCat, new Path(OUTPUT_PATH5));
		if (tvCat.waitForCompletion(true)) {
			System.out.println("JOB FIVE COMPLETION!!");
		}
		
		conf.set("TABLET_RATING", "hdfs://lincoln:30381" + "/Testruns/tablet_rating" ); 

		Job tabletCat = Job.getInstance(conf);
		tabletCat.setJarByClass(MainClass.class);
		tabletCat.setMapperClass(TVExtractMapper.class);
		tabletCat.setReducerClass(TVExtractReducer.class);
		tabletCat.setOutputKeyClass(Text.class);
		tabletCat.setOutputValueClass(Text.class);
		tabletCat.setInputFormatClass(TextInputFormat.class);
		tabletCat.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputDirRecursive(tabletCat, true);

		FileInputFormat.setInputPaths(tabletCat, new Path(OUTPUT_PATH1));//tv rating file 
		FileOutputFormat.setOutputPath(tabletCat, new Path(OUTPUT_PATH6));
		if (tabletCat.waitForCompletion(true)) {
			System.out.println("JOB SIX COMPLETION!!");
		}
	}

}
