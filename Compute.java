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

public class Compute {

        public static class PhraseGeneratorMapper
               extends Mapper<Object, Text, Text, Text>{

                public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {

                        
			Text bigram = new Text();
			Text count = new Text();
			
			String[] line = value.toString().split("\\t");
	                
			bigram.set(line[0]);
			count.set(line[1]);
			
			context.write(bigram, count);
                        
			
                }

        }

        public static class PhraseGeneratorReducer
                extends Reducer<Text,Text,Text,Text> {
		
		private long unigramVocab;
		private long CxTotalCount;
		private long BxTotalCount;
		private long bigramVocab;
		private long CxyTotalCount;
		private long BxyTotalCount;

		public void reduce(Text key, Iterable<Text> values,
                               Context context
                               ) throws IOException, InterruptedException {

                        Configuration conf = context.getConfiguration();
			unigramVocab = conf.getLong("unigramVocabSize", 0);
			CxTotalCount = conf.getLong("unigramForeGroundCountSize", 0);
			BxTotalCount = conf.getLong("unigramBackGroundCountSize", 0);
			bigramVocab = conf.getLong("bigramVocabSize", 0);
			CxyTotalCount = conf.getLong("bigramForeGroundCountSize", 0);
			BxyTotalCount = conf.getLong("bigramBackGroundCountSize", 0);
			
			long Cxy = 0;
			long Bxy = 0;
			long Cx = 0;
			long Bx = 0;
			long Cy = 0;
			long By = 0;
						                        
			for (Text val : values) {
				String[] counts = val.toString().split(",");
				if (counts[0].contains("Bxy")) {
					Bxy = Long.parseLong(counts[0].substring(4, counts[0].length()));
					Cxy = Long.parseLong(counts[1].substring(4, counts[1].length())); 	                               
				}
				else if (counts[0].contains("By")) {
					By = Long.parseLong(counts[0].substring(3, counts[0].length()));
					Cy = Long.parseLong(counts[1].substring(3, counts[1].length())); 					
				}
				else {
					Bx = Long.parseLong(counts[0].substring(3, counts[0].length()));
					Cx = Long.parseLong(counts[1].substring(3, counts[1].length())); 					
				}
			}

                        double pfgXY, pfgX, pfgY, phraseNess;
			pfgXY = (double) (Cxy+1)/(CxyTotalCount + bigramVocab);
			pfgX = (double) (Cx+1)/(CxTotalCount + unigramVocab);
			pfgY = (double) (Cy+1)/(CxTotalCount + unigramVocab);
			phraseNess = (pfgXY)*Math.log(pfgXY/(pfgX*pfgY));

			double pbgXY, informativeNess;
			pbgXY = (double) (Bxy+1)/(BxyTotalCount + bigramVocab);
			informativeNess = (pfgXY)*Math.log(pfgXY/pbgXY);  
                        
			double totalScore = phraseNess + informativeNess;
			
			String scores = totalScore + "\t" + phraseNess + "\t" + informativeNess;
						
			context.write(key, new Text(scores)); 
			
                
                }

        }

}
                                  


