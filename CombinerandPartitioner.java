import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ComPat {

	public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{

 private final static IntWritable one = new IntWritable(1);
 private Text word = new Text();

 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString());
   while (itr.hasMoreTokens()) {
     word.set(itr.nextToken());
     context.write(word, one);
   }
 }
}

public static class MyPartitioner extends Partitioner <Text, IntWritable>{

@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			
			String mykey = key.toString().toLowerCase();
	
			if (mykey.equals("amit")) {
				return 0;
			}
			if (mykey.equals("hadoop")) {
				return 1;
			}
			if (mykey.equals("bhavna")) {
				return 2;
			}
			else {
				return 3;
			}
	
		}	
	
}	
	
	
public static class IntSumReducer
extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result = new IntWritable();

 public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
   int sum = 0;
   for (IntWritable val : values) {
     sum += val.get();
   }
   result.set(sum);
   context.write(key, result);
 }
}
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    
    job.setNumReduceTasks(4);
    job.setJarByClass(ComPat.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setPartitionerClass(MyPartitioner.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    
    /* to rename output file and put in a proper name instead of part-r-0000   */
    
    /*LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);*/
    
    
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
