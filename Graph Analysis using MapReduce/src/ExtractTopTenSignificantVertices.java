/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 3 *********************
  *****************************************
  *****************************************
  */

import java.io.*;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ExtractTopTenSignificantVertices extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new ExtractTopTenSignificantVertices(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		//String input = "/scr/cchsu/lab3/exp1/test/test case 5.txt";
		String input = "/class/s17419/lab3/patents.txt";	//	given citation graph
		String temp1 = "/scr/cchsu/lab3/exp1/temp1";
		String temp2 = "/scr/cchsu/lab3/exp1/temp2";
		String output = "/scr/cchsu/lab3/exp1/output/";

		
		int reduce_tasks = 2;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();
		
		// Create job for round 1
		
		// Create the job
		Job job_one = new Job(conf, "Extract Top 10 Significant Patents Program - Round One"); 
		
		// Attach the job to this Driver
		job_one.setJarByClass(ExtractTopTenSignificantVertices.class); 
		
		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);
		
		// The datatype of the Output Key 
		// Must match with the declaration of the Reducer Class         
		job_one.setOutputKeyClass(Text.class); 
		
		// The datatype of the Output Value 
		// Must match with the declaration of the Reducer Class
		job_one.setOutputValueClass(Text.class);
		
		// The class that provides the map method
		job_one.setMapperClass(Map_One.class); 
		//declare the output types for Mapper
				//job_one.setMapOutputKeyClass(Text.class);
				//job_one.setMapOutputValueClass(Text.class);
		// The class that provides the reduce method
		job_one.setReducerClass(Reduce_One.class);
		
		// Decides how the input will be split
		// We are using TextInputFormat which splits the data line by line
		// This means each map method receives one line as an input
		job_one.setInputFormatClass(TextInputFormat.class);  
		
		// Decides the Output Format
		job_one.setOutputFormatClass(TextOutputFormat.class);
		
		// The input HDFS path for this job
		// The path can be a directory containing several files
		// You can add multiple input paths including multiple directories
		FileInputFormat.addInputPath(job_one, new Path(input)); 
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path)); // This is legal
		
		// The output HDFS path for this job
		// The output path must be one and only one
		// This must not be shared with other running jobs in the system
		FileOutputFormat.setOutputPath(job_one, new Path(temp1));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed
		
		// Run the job
		job_one.waitForCompletion(true); 
		
		
		// Create job for round 2
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
		
		Job job_two = new Job(conf, "Extract Top 10 Significant Patents Program - Round Two"); 
		job_two.setJarByClass(ExtractTopTenSignificantVertices.class); 
		job_two.setNumReduceTasks(reduce_tasks); 
		
		job_two.setOutputKeyClass(Text.class); 
		job_two.setOutputValueClass(IntWritable.class);
		
		// If required the same Map / Reduce classes can also be used
		// Will depend on logic if separate Map / Reduce classes are needed
		// Here we show separate ones
		job_two.setMapperClass(Map_Two.class); 
		job_two.setReducerClass(Reduce_Two.class);
		
		job_two.setInputFormatClass(TextInputFormat.class); 
		job_two.setOutputFormatClass(TextOutputFormat.class);
		//declare the output types for Mapper
		job_two.setMapOutputKeyClass(Text.class);
		job_two.setMapOutputValueClass(Text.class);
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(temp1)); 
		FileOutputFormat.setOutputPath(job_two, new Path(temp2));
		
		// Run the job
		job_two.waitForCompletion(true); 
		
		// Create job for round 3
		// This merges the part files.
		reduce_tasks = 1;
		Job job_three = new Job(conf, "Extract Top 10 Significant Patents Program - Round Three"); 
		job_three.setJarByClass(ExtractTopTenSignificantVertices.class); 
		job_three.setNumReduceTasks(reduce_tasks); 
		
		job_three.setOutputKeyClass(Text.class); 
		job_three.setOutputValueClass(LongWritable.class);
		
		job_three.setSortComparatorClass(LongWritable.DecreasingComparator.class);	//sort by key in decreasing order
		job_three.setMapperClass(Map_Three.class); 
		job_three.setReducerClass(Reduce_Three.class);
		
		job_three.setInputFormatClass(TextInputFormat.class); 
		job_three.setOutputFormatClass(TextOutputFormat.class);
		//declare the output types for Mapper
		job_three.setMapOutputKeyClass(LongWritable.class);
		job_three.setMapOutputValueClass(Text.class);
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_three, new Path(temp2)); 
		FileOutputFormat.setOutputPath(job_three, new Path(output));
		
		// Run the job
		job_three.waitForCompletion(true); 		
		
		return 0;
		
	} // End run
	
	// The Map Class
	// The input to the map method would be a LongWritable (long) key and Text (String) value
	// Notice the class declaration is done with LongWritable key and Text value
	// The TextInputFormat splits the data line by line.
	// The key for TextInputFormat is nothing but the line number and hence can be ignored
	// The value for the TextInputFormat is a line of text from the input
	// The map method can emit data using context.write() method

	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {
		
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			
			String line = value.toString();
			String[] vertices = line.split("[\\s]+");	//split the vertices
			if (Integer.parseInt(vertices[0]) != Integer.parseInt(vertices[1])) {	//ignore 1-hop self-citations 
				context.write(new Text(vertices[1]), new Text("<- " + vertices[0]));	//key = cited, value = citing 
				context.write(new Text(vertices[0]), new Text("-> " + vertices[1]));	//key = citing, value = cited

			}
			
			
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class
	// The key is Text and must match the datatype of the output key of the map method
	// The value is tText and also must match the datatype of the output value of the map method
	public static class Reduce_One extends Reducer<Text, Text, Text, Text>  {
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {

			Set<String> toVertices = new HashSet<String>();	//vertices that can go to the key (node)
			Set<String> fromVertices = new HashSet<String>();	//vertices that can be reached by the key (node)
			for (Text vertex : values) {	//put the neighbors into respective sets by direction
				String direction = vertex.toString().split("[\\s]+")[0];
				if (direction.charAt(0) == '-') {
					toVertices.add(vertex.toString().split("[\\s]+")[1]);
				}
				else {
					fromVertices.add(vertex.toString().split("[\\s]+")[1]);
				}
				if (key.toString().compareTo(vertex.toString().split("[\\s]+")[1]) == 0)
					throw new IllegalArgumentException("1-hop self-loop");
			}
			for (String toVertex : toVertices) {	
				context.write(new Text(toVertex), new Text(key));	//emit vertices can be reached by key (node)
				for (String fromVertex : fromVertices) {	//vertices that can reach the key (node)
					if (toVertex.compareTo(fromVertex) == 0)	//ignore 2-hop self-loop
						continue;
					context.write(new Text(toVertex), new Text(fromVertex));	//emit 2-hop vertices
				}
				// Use context.write to emit values
			}
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, Text, Text>  {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			
			String line = value.toString();
			String[] vertices = line.split("[\\s]+");	//split the vertices
			context.write(new Text(vertices[0]), new Text(vertices[1]));	//key = cited, value = citing
			//if (vertices[0].compareTo(vertices[1]) == 0)
			//	throw new IllegalArgumentException("2-hop self loop");
		}  // End method "map"
		
	}  // End Class Map_Two
	
	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, Text, Text, IntWritable>  {
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			Set<String> vertices = new HashSet<String>();	//collecting distinct citing nodes
			for (Text val : values) {	//count the citation number
				//context.progress();
				vertices.add(val.toString());
				
			}
			
			context.write(key, new IntWritable(vertices.size()));	//key = node, value = times of citation
		}  // End method "reduce"
		
	}  // End Class Reduce_Two
	
	
	// The third Map Class
	public static class Map_Three extends Mapper<LongWritable, Text, LongWritable, Text>  {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			
			String line = value.toString();
			//key = citations, value = node, so that we can sort by the citation counts
			context.write(new LongWritable(Integer.parseInt(line.split("[\\s]+")[1])), new Text(line.split("[\\s]+")[0]));
		}  // End method "map"
		
	}  // End Class Map_Three
	
	// The third Reduce class
	public static class Reduce_Three extends Reducer<LongWritable, Text, Text, LongWritable>  {
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			for (Text vertex : values)
				context.write(new Text(vertex), key);	//reverse the presentation back to key = node, value = counts
		}  // End method "reduce"
		
	}  // End Class Reduce_Three
	
}