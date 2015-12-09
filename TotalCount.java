import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalCount {

public static class TCMapper
extends Mapper<Object, Text, NullWritable, IntWritable>{


 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString());
   IntWritable cnt = new IntWritable(itr.countTokens());
   context.write(NullWritable.get(), cnt); 
 
 }
}

public static class TCReducer
extends Reducer<NullWritable,IntWritable,NullWritable,IntWritable> {
 private IntWritable result = new IntWritable();

 
public void reduce(NullWritable key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
   int sum = 0;
   for (IntWritable val : values) {
     sum += val.get();
   }
   result.set(sum);
   context.write(key, result);
   
 }
 
 String GenerateFileName()
 {
	 return "MyReducedFile.txt";
 }
 
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "total count");
    job.setJarByClass(TotalCount.class);
    job.setMapperClass(TCMapper.class);
    job.setCombinerClass(TCReducer.class);
    job.setReducerClass(TCReducer.class);
    job.setOutputKeyClass(NullWritable.class);
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
