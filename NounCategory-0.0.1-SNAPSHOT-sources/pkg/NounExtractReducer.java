package pkg;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NounExtractReducer extends Reducer<Text, Text,Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		ArrayList<String> noun = new ArrayList<String>();// for nouns/propernouns/adjectives
		ArrayList<String> proper_noun = new ArrayList<String>();
		
		for(Text val: values){
			String text = val.toString() ;
			
			String[] words = text.split(" ");
			
			for(String word: words){
				if(word.endsWith("/NN")){
					word = word.substring(0, word.length()-3);
					noun.add(word);
				}
				
				if(word.endsWith("/NNP")){
					word = word.substring(0,word.length()-4);
					proper_noun.add(word);
					
				}
			}
		}
		String nword = "";
		for(String w : noun){
			
			nword = nword +" "+ w;
		}
		nword = nword.trim();
		String pronoun = "";
		for(String p : proper_noun){
			
			pronoun = pronoun + " "+ p;
		}
		pronoun = pronoun.trim();
		context.write(key, new Text(nword +" "+ pronoun));
	}

}
