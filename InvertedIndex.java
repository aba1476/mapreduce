import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	
	public static class IndexMap extends Mapper <LongWritable, Text, Text, Text> {
		
		private Text documentId;
		private Text word = new Text();
		
		public void setup (Context context) {
			String fileName = ((FileSplit)(context.getInputSplit())).getPath().getName(); 
			
			documentId = new Text(fileName);
		}
		
		public void map (LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());	
		
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, documentId);
		}
			
		}
	}
	
	
	public static class IndexReduce extends Reducer<Text, Text, Text, Text> {
		private Text docIds = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			HashSet<String> uniqueDocIds = new HashSet<String>();
			
			for (Text id1 : values) {
				uniqueDocIds.add(id1.toString());
			}
			
			docIds.set(new Text(StringUtils.join(uniqueDocIds, ",")));
			
			context.write(key, docIds);
			
		}
		
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Inverted Index");
	    job.setJarByClass(InvertedIndex.class);
	    job.setMapperClass(IndexMap.class);
	    job.setReducerClass(IndexReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
		
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    
	    /* check if output directory exists or not */
	    
	    FileSystem fs = FileSystem.get(conf);
	    
	    if (fs.exists(new Path(args[1]))) {
	    	fs.delete(new Path(args[1]), true);
	    }	
	    
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	    
		
	}
	
	
	

}
