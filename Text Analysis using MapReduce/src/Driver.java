/**
  *****************************************
  *****************************************
  * Cpr E 419 - Lab 2 *********************
  * For question regarding this code,
  * please ask on Piazza
  *****************************************
  *****************************************
  */

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		//String input = "/class/s17419/lab2/shakespeare";    // testing input (exp1)
		String input = "/class/s17419/lab2/gutenberg";	// larger dataset (exp2)
		String temp1 = "/scr/cchsu/lab2/exp2/temp1";
		String output = "/scr/cchsu/lab2/exp2/output/";
		String temp2 = "/scr/cchsu/lab2/exp2/temp2";     
		//String output = "/scr/cchsu/lab2/exp2/output/";
		
		int reduce_tasks = 2;  // The number of reduce tasks that will be assigned to the job
		Configuration conf = new Configuration();
		
		// Create job for round 1
		
		// Create the job
		Job job_one = new Job(conf, "Driver Program Round One"); 
		
		// Attach the job to this Driver
		job_one.setJarByClass(Driver.class); 
		
		// Fix the number of reduce tasks to run
		// If not provided, the system decides on its own
		job_one.setNumReduceTasks(reduce_tasks);
		
		// The datatype of the Output Key 
		// Must match with the declaration of the Reducer Class         
		job_one.setOutputKeyClass(Text.class); 
		
		// The datatype of the Output Value 
		// Must match with the declaration of the Reducer Class
		job_one.setOutputValueClass(IntWritable.class);
		
		// The class that provides the map method
		job_one.setMapperClass(Map_One.class); 
		
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
		
		Job job_two = new Job(conf, "Driver Program Round Two"); 
		job_two.setJarByClass(Driver.class); 
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
		Job job_three = new Job(conf, "Driver Program Round Three"); 
		job_three.setJarByClass(Driver.class); 
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
	// However, to match the class declaration, it must emit Text as key and IntWribale as value
	public static class Map_One extends Mapper<LongWritable, Text, Text, IntWritable>  {
		
		// The map method 
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
			
			// The TextInputFormat splits the data line by line.
			// So each map method receives one line from the input
			String line = value.toString();
			String[] words = line.split("[.!?\\s]+");	//split the line by specified delimiters
			String firstWordOfBigram = null;	//the first word of the bigram, initially 0
			
			for (String word : words) {	//read in word by word
				
				word = word.toLowerCase().replaceAll("[^a-z0-9]", "");	//convert to lowercase and remove punctuations
				if (word != null && !word.isEmpty()) {	//check if it is still a valid word
					if (firstWordOfBigram == null) {	//assign first word
						firstWordOfBigram = word;
						continue;
					}
					else {	//combine the current word with the previous word as bigram and output the pair
						context.write(new Text(firstWordOfBigram + " " + word), new IntWritable(1));
						firstWordOfBigram = word;	//reassign the first word of bigram
					}
				}
					
			} 
			
			
			
		} // End method "map"
		
	} // End Class Map_One
	
	
	// The reduce class
	// The key is Text and must match the datatype of the output key of the map method
	// The value is IntWritable and also must match the datatype of the output value of the map method
	public static class Reduce_One extends Reducer<Text, IntWritable, Text, IntWritable>  {
		
		// The reduce method
		// For key, we have an Iterable over all values associated with this key
		// The values come in a sorted fasion.
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
											throws IOException, InterruptedException  {
			int sum = 0;	//count the occurences of bigrams
			for (IntWritable val : values) {
				context.progress();
				sum += val.get();
				
			}
			
			context.write(key, new IntWritable(sum));
			
			// Use context.write to emit values
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	// The second Map Class
	public static class Map_Two extends Mapper<LongWritable, Text, Text, Text>  {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			
			String line = value.toString();
			// output <first letter of bigram, bigram> as <key, value> pair
			context.write(new Text(line.substring(0, 1)), new Text(line));
		}  // End method "map"
		
	}  // End Class Map_Two
	
	// The second Reduce class
	public static class Reduce_Two extends Reducer<Text, Text, Text, IntWritable>  {
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			int maxFrequency = 0;	//record the number of occurrence of the most frequent bigram with the same letter (key)
			String frequencistBigram = null;	//record the bigram appears the most with the same letter (key)
			for (Text initial : values) {	//iterate through the bigram with the same letter (key)
				context.progress();	//prevent timeout dump
				String value = initial.toString();
				
				String[] words = value.split("\\s+");	//split the line into three parts from the format (word1 word2 occurrence)
				if (words.length != 3) {
					throw new IllegalArgumentException("format was not as expected");
				}
				if (Integer.parseInt(words[2]) > maxFrequency) {	//update the most frequent bigram and the occurrence
					frequencistBigram = words[0] + " " + words[1];
					maxFrequency = Integer.parseInt(words[2]);
				}
				
			}
			context.write(new Text(key + " - " + frequencistBigram + ","), new IntWritable(maxFrequency));	//out out the bigram for the letter
		}  // End method "reduce"
		
	}  // End Class Reduce_Two
	
	
	// The third Map Class
	public static class Map_Three extends Mapper<LongWritable, Text, Text, Text>  {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException  {
			
			
			String line = value.toString();
			// read line and output it
			context.write(new Text(line), new Text(""));
		}  // End method "map"
		
	}  // End Class Map_Three
	
	// The third Reduce class
	public static class Reduce_Three extends Reducer<Text, Text, Text, Text>  {
		
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException  {
			
			context.write(new Text(""), new Text(key));	//out out the bigram for the letter
		}  // End method "reduce"
		
	}  // End Class Reduce_Three
	
}