import java.io.IOException;
import java.util.Vector;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Aggregate {

	private static String[] stopwords = {"i", "the", "to", "and", "a", "an", "of", "it", "you", "that", "in", "my", "is", "was", "for"};
	private static HashSet<String> stopWords = new HashSet<String>(Arrays.asList(stopwords));

	public static class AggregateMapper
	       extends Mapper<Object, Text, Text, Text>{

		private Text nGram = new Text();
		private Text rawData = new Text();

		public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
      
 			String[] line = value.toString().split("\\t", 2);
			
			//Check for StopWords	
			String[] words = line[0].split("\\s+");
			for (int i = 0; i < words.length; i++)			
				if (stopWords.contains(words[i]))
					return;

			nGram.set(line[0]);
			rawData.set(line[1]);
			context.write(nGram, rawData);			
		}
  	}

  	public static class AggregateReducer
       		extends Reducer<Text,Text,Text,Text> {
		
		private Text counts = new Text();
				    		
		public void reduce(Text key, Iterable<Text> values,
                	       Context context
                	       ) throws IOException, InterruptedException {
      			
			
			int year;
			long count;
			long fgCount = 0, bgCount = 0;
			String[] rawData;
			
			for (Text val : values) {
        			
                                rawData = val.toString().split("\\t");
				year = Integer.parseInt(rawData[0]);
                                count = Long.parseLong(rawData[1]);
                        	
                                if (year == 1960) 
                                        fgCount = count;
                                        
                                else
                                        bgCount = bgCount + count;
                        }
			
			String[] words = key.toString().split(" ");
			if (words.length == 1)
				counts.set("Bx="+String.valueOf(bgCount)+","+"Cx="+String.valueOf(fgCount));
			else 
				counts.set("Bxy="+String.valueOf(bgCount)+","+"Cxy="+String.valueOf(fgCount));
			context.write(key, counts);
				
		
    		}
  	}

}
