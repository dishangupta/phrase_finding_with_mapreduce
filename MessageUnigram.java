import java.io.IOException;
import java.util.Vector;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MessageUnigram {

	public static class UnigramCombinerMapper
	       extends Mapper<Object, Text, Text, Text>{

		private Text unigram = new Text();
                private Text valueText = new Text();

                public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {

                        String[] line = value.toString().split("\\t");
                        String[] words = line[0].split("\\s+");
			
			if (words.length == 1) {
				unigram.set(words[0]);
				valueText.set(line[1]);
				context.write(unigram, valueText);
			}
			
			else {
				for (int i = 0; i < words.length; i++) {
					unigram.set(words[i]);
					valueText.set(line[0]);
					context.write(unigram, valueText);
					
					//Check if both words in bigram are same	
					if (words[0].equals(words[1]))
						break;				
				}
					
			}
		
                }

  	}

  	public static class UnigramCombinerReducer
       		extends Reducer<Text,Text,Text,Text> {
    		
		
                public void reduce(Text key, Iterable<Text> values,
                               Context context
                               ) throws IOException, InterruptedException {
			
                        List<String> bigrams = new ArrayList<String>();
			String BxCx = "";
			String ByCy = "";
			                        
                        for (Text val : values) {
				
				if (val.toString().contains(","))
					BxCx = val.toString();
				else
					bigrams.add(val.toString());
					                                
			}
			
			ByCy = BxCx.replaceAll("x", "y");
			Text phrase = new Text();
			Text unigramCount = new Text();
	
			String[] words;
			for (String bigram : bigrams) {
				words = bigram.toString().split("\\s+");				
				phrase.set(bigram);
				
				if (key.toString().equals(words[0])) { 
					unigramCount.set(BxCx);
					context.write(phrase, unigramCount);
				}
				if (key.toString().equals(words[1])) {
					unigramCount.set(ByCy);
					context.write(phrase, unigramCount);
				}
					
			}
			
				
        	}

  	}

}
