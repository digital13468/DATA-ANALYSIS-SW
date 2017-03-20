import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class TopTweeters {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	    
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
	    int reduce_tasks = 2;
		
        // Create a Hadoop Job
		Job job = Job.getInstance(conf, "Follower Count");
        
        // Attach the job to this Class
		job.setJarByClass(TopTweeters.class); 
        
        // Number of reducers
		job.setNumReduceTasks(reduce_tasks);
        
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(IntWritable.class);	// sort by value
        
        // Set the Map class
		job.setMapperClass(Map.class); 
        job.setReducerClass(Reduce.class);
        
        // Set how the input is split
        // TextInputFormat.class splits the data per line
		job.setInputFormatClass(JsonInputFormat.class); 
        
        // Output format class
		job.setOutputFormatClass(TextOutputFormat.class);
        
        // Input path
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 
        
        // Output path
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        // Run the job
		job.waitForCompletion(true);
		// sort by followers_count
		new SortByValueDescending().sortByValueDescendingInt(conf, TopTweeters.class, new Path(otherArgs[1]), new Path(otherArgs[2]));
	} 

	
    // The Map Class
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>  {
		
        // The map method 
		public void map(LongWritable key, Text value, Context context)
							throws IOException, InterruptedException  {
			
			
			JSONParser parser = new JSONParser();	
			JSONObject jobj;
			
			try {
				jobj = (JSONObject) parser.parse(value.toString());	// parse the returned record in JSON 
				JSONObject user = (JSONObject) jobj.get("user");	// find user field
				String screenName = user.get("screen_name").toString();	// find screen_name field
				String followersCount = user.get("followers_count").toString();	// find followers_count field
					
				// emit screen_name, followers_count
				context.write(new Text(screenName), new IntWritable(Integer.parseInt(followersCount)));
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		} 	
	} 
	
    // The Reduce class
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>  {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
			int maxCount = 0;
			for (IntWritable val : values) {	// look for the max followers_count of the user across his/her tweets
				if (val.get() > maxCount)
					maxCount = val.get();
				//context.write(key, values);
			}
			context.write(key, new IntWritable(maxCount)); // emit screen_name, max followers_count
		} 
	}
}
