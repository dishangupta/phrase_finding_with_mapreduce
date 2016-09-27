import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CountSize {
	
	public static class CountSizeMapper
	       extends Mapper<Object, Text, Text, LongWritable>{

		private Text vocab = new Text();
		private Text foreground = new Text();
		private Text background = new Text();

		private LongWritable one = new LongWritable(1);
	
                public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {

                        String[] line = value.toString().split("\\t");
			String[] words = line[0].split("\\s+");

			String[] counts;
			long fgCount, bgCount;                        
			
			//Map counts for vocab, foreground and background
			if (words.length == 1) {
				vocab.set("unigramVocabSize");
				foreground.set("unigramForeGroundCountSize");
				background.set("unigramBackGroundCountSize");
																									
				counts = line[1].split(",");
				bgCount = Long.parseLong(counts[0].substring(3, counts[0].length()));
				fgCount = Long.parseLong(counts[1].substring(3, counts[1].length()));
			}		
			
			else {
				vocab.set("bigramVocabSize");
				foreground.set("bigramForeGroundCountSize");
				background.set("bigramBackGroundCountSize");
				
				counts = line[1].split(",");
				bgCount = Long.parseLong(counts[0].substring(4, counts[0].length()));
				fgCount = Long.parseLong(counts[1].substring(4, counts[1].length()));
			}		
			
			context.write(vocab, one);
			context.write(foreground, new LongWritable(fgCount));
			context.write(background, new LongWritable(bgCount));
		}
       	}
			
	public static class CountSizeReducer
		extends Reducer<Text,LongWritable,Text,LongWritable> {
		
		private LongWritable result = new LongWritable();
			
		public void reduce(Text key, Iterable<LongWritable> values,
			Context context
			) throws IOException, InterruptedException {
			
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
	}	
}

