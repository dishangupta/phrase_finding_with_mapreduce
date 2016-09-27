import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class run_hadoop_phrase {

	public static Map<String, Long> getCountsFromFile (String filePath) 
		throws Exception {
		Map<String, Long> sizeCounts = new HashMap<String, Long>();		
		
		try{
			String uri = filePath;    
			FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());        		
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
            		String line;
						
			while ((line = br.readLine()) != null) {            		
		      		
				String[] sizecount = line.split("\t");
                		sizeCounts.put(sizecount[0], Long.parseLong(sizecount[1]));
            		}
        	} 
		
		catch(Exception e){
			System.err.println("ERROR! Could not read from file");
		}
		
		return sizeCounts;
	}

	
	public static void main(String[] args) throws Exception {
    
		boolean isSuccessful;
		//Read input arguments
		String unigramFile = args[0];
		String bigramFile = args[1];
		String aggregatedFolder = args[2];
		String sizecountFolder = args[3];
		String unigramMessageFolder = args[4];
		String finalFolder = args[5];		    	
		
		Configuration conf = new Configuration();
		
		//Aggregate unigram counts
		Job job = Job.getInstance(conf, "AggregateUnigram");
		job.setJarByClass(run_hadoop_phrase.class);
		job.setMapperClass(Aggregate.AggregateMapper.class);
		//job.setCombinerClass(Aggregate.AggregateReducer.class);
		job.setReducerClass(Aggregate.AggregateReducer.class);
		job.setNumReduceTasks(10);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(unigramFile));
		FileOutputFormat.setOutputPath(job, new Path(aggregatedFolder+"/unigram_processed"));
		
		try {
			isSuccessful = job.waitForCompletion(true);
	    	}	    	
		catch (Exception e) {
			System.err.println("ERROR! Could not finish job.");
		}	    		   		   		    		    		    		    		    		  
   		
		//Aggregate bigram counts
   		job = Job.getInstance(conf, "AggregateBigram");
                job.setJarByClass(run_hadoop_phrase.class);
                job.setMapperClass(Aggregate.AggregateMapper.class);
                //job.setCombinerClass(Aggregate.AggregateReducer.class);
                job.setReducerClass(Aggregate.AggregateReducer.class);
                job.setNumReduceTasks(10);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job, new Path(bigramFile));
                FileOutputFormat.setOutputPath(job, new Path(aggregatedFolder+"/bigram_processed"));
                try {
                        isSuccessful = job.waitForCompletion(true);
                } 
		catch (Exception e) {  
	                System.err.println("ERROR! Could not finish job.");
                }

		//Calculate size counts
 		job = Job.getInstance(conf, "SizeCounts");
                job.setJarByClass(run_hadoop_phrase.class);
		job.setMapperClass(CountSize.CountSizeMapper.class);
                //job.setCombinerClass(Aggregate.AggregateReducer.class);
                job.setReducerClass(CountSize.CountSizeReducer.class);
                job.setNumReduceTasks(1);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);
                FileInputFormat.addInputPath(job, new Path(aggregatedFolder+"/unigram_processed")); 
		FileInputFormat.addInputPath(job, new Path(aggregatedFolder+"/bigram_processed"));
                FileOutputFormat.setOutputPath(job, new Path(sizecountFolder));
                try {
                	isSuccessful = job.waitForCompletion(true);
                }
                catch (Exception e) {
                	System.err.println("ERROR! Could not finish job.");
               	}
		
		//Combine messages with unigram counts
		job = Job.getInstance(conf, "MessageUnigramCombiner");
                job.setJarByClass(run_hadoop_phrase.class);
                job.setMapperClass(MessageUnigram.UnigramCombinerMapper.class);
                //job.setCombinerClass(Aggregate.AggregateReducer.class);
                job.setReducerClass(MessageUnigram.UnigramCombinerReducer.class);
                job.setNumReduceTasks(10);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path(aggregatedFolder+"/unigram_processed"));
		FileInputFormat.addInputPath(job, new Path(aggregatedFolder+"/bigram_processed"));
		FileOutputFormat.setOutputPath(job, new Path(unigramMessageFolder));
                try {
                        isSuccessful = job.waitForCompletion(true);
                }
		catch (Exception e) {
                        System.err.println("ERROR! Could not finish job.");
                }
		
		//Read size counts from file and set configuration
		Map<String, Long> sizeCounts;
		sizeCounts = getCountsFromFile(sizecountFolder+"/part-r-00000"); 		
		
		for (String s : sizeCounts.keySet())
			conf.setLong(s, sizeCounts.get(s));
                		
		//Combine unigram counts with bigram counts
		//and compute relevant phrases
		job = Job.getInstance(conf, "PhraseGenerator");
                job.setJarByClass(run_hadoop_phrase.class);
                job.setMapperClass(Compute.PhraseGeneratorMapper.class);
		//job.setCombinerClass(Aggregate.AggregateReducer.class);
		job.setReducerClass(Compute.PhraseGeneratorReducer.class);
                job.setNumReduceTasks(10);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
            	FileInputFormat.addInputPath(job, new Path(aggregatedFolder+"/bigram_processed"));
                FileInputFormat.addInputPath(job, new Path(unigramMessageFolder));
                FileOutputFormat.setOutputPath(job, new Path(finalFolder));
                try {
                        isSuccessful = job.waitForCompletion(true);
                }
                catch (Exception e) {
                        System.err.println("ERROR! Could not finish job.");
                }


	}
}
