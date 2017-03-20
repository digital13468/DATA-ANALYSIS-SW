import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class SortByValueDescending {
	
	boolean sortByValueDescendingInt(Configuration conf, Class<?> cls, Path input, Path output) throws Exception {
		int reduce_tasks = 1;
		
	    // Create a Hadoop Job
		Job job = Job.getInstance(conf, "Sort By Value Descending Int");
	    
	    // Attach the job to this Class
		job.setJarByClass(cls); 
	    
	    // Number of reducers
		job.setNumReduceTasks(reduce_tasks);
	    
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(IntWritable.class);
		job.setSortComparatorClass(IntComparator.class);
	    // Set the Map class
		job.setMapperClass(MapInt.class); 
	    job.setReducerClass(ReduceInt.class);
	    
	    // Set how the input is split
	    // TextInputFormat.class splits the data per line
		job.setInputFormatClass(TextInputFormat.class); 
	    
	    // Output format class
		job.setOutputFormatClass(TextOutputFormat.class);
	    
	    // Input path
		FileInputFormat.addInputPath(job, input); 
	    
	    // Output path
		FileOutputFormat.setOutputPath(job, output);
	    
	    // Run the job
		return job.waitForCompletion(true);
	}
	public static class IntComparator extends WritableComparator {
	    public IntComparator() {
	    	super(IntWritable.class);
	    }
	    
	    @Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    	
	    	Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
	    	Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
	    	
	    	return v1.compareTo(v2) * (-1);
	    }
	}
	// The Map Class
	public static class MapInt extends Mapper<LongWritable, Text, IntWritable, Text>  {
		
        // The map method 
		public void map(LongWritable key, Text value, Context context)
							throws IOException, InterruptedException  {
			String[] line = value.toString().split("[\\s]+");	// separate text and number
			int number = Integer.parseInt(line[1]);
			String text = line[0];
			context.write(new IntWritable(number), new Text(text));	// number, text
			//context.write(new IntWritable(), new Text(value.toString().split("[\\s]+")[0]));
		} 	
	} 
	
    // The Reduce class
	public static class ReduceInt extends Reducer<IntWritable, Text, Text, IntWritable>  {
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
			//context.write(new Text(key.toString()), values);
			//int number = key.get();
			for(Text val : values)
				context.write(val, key);	// text, number
		} 
	}
}
