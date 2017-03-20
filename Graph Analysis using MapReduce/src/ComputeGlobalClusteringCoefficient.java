/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 3 *********************
  *****************************************
  *****************************************
  */

import java.util.List;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ComputeGlobalClusteringCoefficient extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new ComputeGlobalClusteringCoefficient(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		//String input = "/scr/cchsu/lab3/exp2/test/TestCase1.txt";
		String input = "/class/s17419/lab3/patents.txt";	//	given citation graph
		String tripletDict = "/scr/cchsu/lab3/exp2/triplets/output";	//store triple number
		String tripletTemp1 = "/scr/cchsu/lab3/exp2/triplets/temp1";	//store intermidiate results (job 2)
		String triangleDict = "/scr/cchsu/lab3/exp2/triangles/output";	//store triangle number
		String triangleTemp1 = "/scr/cchsu/lab3/exp2/triangles/temp1";	//store intermediate results (job 3)
		String triangleTemp2 = "/scr/cchsu/lab3/exp2/triangles/temp2";	//store intermediate results (job 4)
		String output = "/scr/cchsu/lab3/exp2/output/";	//final result (the coefficient)

		
		int reduce_tasks = 2;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();
		
		// Create job for round 1
		
		// Create the job
		Job job_one = new Job(conf, "Compute Global Clustering Coefficient Program - Round One"); 
		
		// Attach the job to this Driver
		job_one.setJarByClass(ComputeGlobalClusteringCoefficient.class); 
		
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
		FileOutputFormat.setOutputPath(job_one, new Path(tripletTemp1));
		//divide the work into two groups; one for triplet count, one for triangle
		MultipleOutputs.addNamedOutput(job_one, "triangle", TextOutputFormat.class, Text.class, Text.class);
		// Run the job
		job_one.waitForCompletion(true); 
		
		
		// Create job for round 2
		// The output of the previous job can be passed as the input to the next
		// The steps are as in job 1
		reduce_tasks = 1;
		Job job_two = new Job(conf, "Compute Triplets Program - Round One"); 
		job_two.setJarByClass(ComputeGlobalClusteringCoefficient.class); 
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
		job_two.setMapOutputValueClass(IntWritable.class);
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_two, new Path(tripletTemp1)); 
		FileOutputFormat.setOutputPath(job_two, new Path(tripletDict));
		
		// Run the job
		job_two.submit();
		
		// Create job for round 3
		// This merges the part files.
		reduce_tasks = 2;
		Job job_three = new Job(conf, "Compute Triangles Program - Round One"); 
		job_three.setJarByClass(ComputeGlobalClusteringCoefficient.class); 
		job_three.setNumReduceTasks(reduce_tasks); 
		
		job_three.setOutputKeyClass(Text.class); 
		job_three.setOutputValueClass(Text.class);
		
		
		job_three.setMapperClass(Map_Three.class); 
		job_three.setReducerClass(Reduce_Three.class);
		
		job_three.setInputFormatClass(TextInputFormat.class); 
		job_three.setOutputFormatClass(TextOutputFormat.class);
		//declare the output types for Mapper
		job_three.setMapOutputKeyClass(Text.class);
		job_three.setMapOutputValueClass(Text.class);
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_three, new Path(triangleTemp1)); 
		FileOutputFormat.setOutputPath(job_three, new Path(triangleTemp2));
		
		// Run the job
		job_three.waitForCompletion(true); 	
		// Create job for round 4
		// This merges the part files.
		reduce_tasks = 1;
		Job job_four = new Job(conf, "Compute Triangles Program - Round Two"); 
		job_four.setJarByClass(ComputeGlobalClusteringCoefficient.class); 
		job_four.setNumReduceTasks(reduce_tasks); 
		
		job_four.setOutputKeyClass(Text.class); 
		job_four.setOutputValueClass(IntWritable.class);
		
		
		job_four.setMapperClass(Map_Four.class); 
		job_four.setReducerClass(Reduce_Four.class);
		
		job_four.setInputFormatClass(TextInputFormat.class); 
		job_four.setOutputFormatClass(TextOutputFormat.class);
		//declare the output types for Mapper
		job_four.setMapOutputKeyClass(Text.class);
		job_four.setMapOutputValueClass(Text.class);
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_four, new Path(triangleTemp2)); 
		FileOutputFormat.setOutputPath(job_four, new Path(triangleDict));
		
		// Run the job
		job_four.waitForCompletion(true); 
		// Create job for round 5
		
		reduce_tasks = 1;
		Job job_five = new Job(conf, "Compute Global Clustering Coefficient Program - Round Two"); 
		job_five.setJarByClass(ComputeGlobalClusteringCoefficient.class); 
		job_five.setNumReduceTasks(reduce_tasks); 
		
		job_five.setOutputKeyClass(Text.class); 
		job_five.setOutputValueClass(FloatWritable.class);
		
		
		job_five.setMapperClass(Map_Five.class); 
		job_five.setReducerClass(Reduce_Five.class);
		
		job_five.setInputFormatClass(TextInputFormat.class); 
		job_five.setOutputFormatClass(TextOutputFormat.class);
		//declare the output types for Mapper
		job_five.setMapOutputKeyClass(NullWritable.class);
		job_five.setMapOutputValueClass(Text.class);
		// The output of previous job set as input of the next
		FileInputFormat.addInputPath(job_five, new Path(triangleDict));
		FileInputFormat.addInputPath(job_five, new Path(tripletDict)); 
		FileOutputFormat.setOutputPath(job_five, new Path(output));
		
		// Run the job
		job_five.waitForCompletion(true);
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
			//for each edge, emit two pairs of key and value: key = node1 (node2), value = node2 (node1)
			
			String line = value.toString();
			String[] vertices = line.split("[\\s]+");	
			if (vertices[0].compareTo(vertices[1]) != 0) {	
				context.write(new Text(vertices[0]), new Text(vertices[1]));	 
				context.write(new Text(vertices[1]), new Text(vertices[0]));	

			}
			
			
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class
	// The key is Text and must match the datatype of the output key of the map method
	// The value is tText and also must match the datatype of the output value of the map method
	public static class Reduce_One extends Reducer<Text, Text, Text, IntWritable>  {
		MultipleOutputs<Text, Text> mos;
		public void setup (Context context) {	//configure the other output instance
			mos = new MultipleOutputs(context);
		}
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			Set<String> neighbors = new HashSet<String>();
			//collect neighors for key (node) with no duplication
			for (Text val : values) {
				//context.progress();
				//numNeighbor += 1;
				neighbors.add(val.toString());
				
			}
			//if a node has less than two neighbors, it cannot form any triplets nor triangles
			if (neighbors.size() >= 2) {	//count triplet number for key (node)
				context.write(key, new IntWritable(neighbors.size() * (neighbors.size() - 1) / 2));
				for (String neighbor : neighbors)	
					//for each neighbor in the neighbor list, 
					//emit key = the neighbor, value = the same neighbor list as the key (node) has
					mos.write("triangle", neighbor, "{" + key + ", " + 
					neighbors.toString() + "}", "/scr/cchsu/lab3/exp2/triangles/temp1/triangle");
			}
		} // End method "reduce" 
		protected void cleanup (Context context) throws IOException, InterruptedException {
			mos.close();	//close the other output
		}
	} // End Class Reduce_One
	
	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, Text, IntWritable>  {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			//emit the list of the numbers of triplets 
			String line = value.toString();
			String[] vertices = line.split("[\\s]+");	
			context.write(new Text("triplet_number_list"), new IntWritable(Integer.parseInt(vertices[1])));
			
		}  // End method "map"
		
	}  // End Class Map_Two
	
	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, IntWritable, Text, IntWritable>  {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException  {
			int sum = 0;
			for (IntWritable val : values) {
				//context.progress();
				sum += val.get();
				
			}
			//emit the total number of triplets
			context.write(new Text("Total_triplets"), new IntWritable(sum));
		}  // End method "reduce"
		
	}  // End Class Reduce_Two
	
	
	// The third Map Class
	public static class Map_Three extends Mapper<LongWritable, Text, Text, Text>  {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			
			String[] line = value.toString().split("\\{");
			//read the input as key = node, value = {a node connecting to the key (node), (list of other neighbors)}
			context.write(new Text(line[0].trim()), new Text("{" + line[1]));
		}  // End method "map"
		
	}  // End Class Map_Three
	
	// The third Reduce class
	public static class Reduce_Three extends Reducer<Text, Text, Text, Text>  {
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			Set<String> triangleSet = new HashSet<String>();	//store the found triangles
			Map<String, Set<String>> secondNodes = new HashMap<String, Set<String>>();	//store second and third nodes
			for (Text value : values) {
				String list = value.toString();
				Set<String> thirdNodes = new HashSet<String>();
				String sublist = list.substring(list.indexOf("[") + 1, list.indexOf("]"));	//extract third nodes
				String[] neighbor_list = sublist.split(", ");
				for (String neighbor : neighbor_list)	//store the third nodes in a set
					thirdNodes.add(neighbor);	
				secondNodes.put(list.substring(list.indexOf("{") + 1, list.indexOf(",")), thirdNodes);	//for sec. node
			}
			for (String secondNode : secondNodes.keySet()) {
				for (String thirdNode : secondNodes.get(secondNode)) {
					//check whether any of the third nodes also is a second node (distinctively), namely, nbr. of nbr.
					if (secondNodes.containsKey(thirdNode)) {
						List<String> triangleNodes = new ArrayList<String>();
						triangleNodes.add(key.toString().replaceAll("\\p{C}", ""));
						triangleNodes.add(secondNode.replaceAll("\\p{C}", ""));
						triangleNodes.add(thirdNode.replaceAll("\\p{C}", ""));
						Collections.sort(triangleNodes);	//sort the found triangle nodes
						//only consider when the key (node) is also the smallest to avoid duplications
						if (triangleNodes.get(1).equals(key.toString()))
							triangleSet.add(triangleNodes.toString());
					}
				}
			}
			for (String triangle : triangleSet)	//emit the triangles
				context.write(new Text(triangle), new Text(""));	
			
		}  // End method "reduce"
		
	}  // End Class Reduce_Three
	// The four Map Class
	public static class Map_Four extends Mapper<LongWritable, Text, Text, Text>  {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			
			
			//combine all triangles into a list
			context.write(new Text("triangle_list"), value);
		}  // End method "map"
		
	}  // End Class Map_Four
	
	// The four Reduce class
	public static class Reduce_Four extends Reducer<Text, Text, Text, IntWritable>  {
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			int sum = 0;	
			for (Text triangleNodes : values)
				sum ++;
			context.write(new Text("triangle_number"), new IntWritable(sum));	//count and emit the total number
		}  // End method "reduce"
		
	}  // End Class Reduce_Four
	// The fifth Map Class
	public static class Map_Five extends Mapper<LongWritable, Text, NullWritable, Text>  {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			
			//read results of triplet and triangle numbers
			context.write(NullWritable.get(), value);
		}  // End method "map"
		
	}  // End Class Map_Five
	
	// The fifth Reduce class
	public static class Reduce_Five extends Reducer<NullWritable, Text, Text, FloatWritable>  {
		
		public void reduce(NullWritable key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			float numTriplets = 0;
			float numTriangles = 0;
			for (Text value : values) {	//based on the format (string) determine the respective numbers
				String[] line = value.toString().split("[\\s]+");
				if (line[0].equals("Total_triplets"))
					numTriplets = Float.valueOf(line[1]);
				else if (line[0].equals("triangle_number"))
					numTriangles = Float.valueOf(line[1]);
			}
			//compute and emit the global clustering coefficient
			context.write(new Text("global_clustering_coefficient"), new FloatWritable(3 * numTriangles / numTriplets));
			
			//context.write(value, new FloatWritable((float) 1.0));
		}  // End method "reduce"
		
	}  // End Class Reduce_Five
}