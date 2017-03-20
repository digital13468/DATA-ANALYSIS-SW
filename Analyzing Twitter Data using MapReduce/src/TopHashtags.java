import java.io.IOException;
import java.util.HashSet;

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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



public class TopHashtags {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	    
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
	    int reduce_tasks = 2;
		
        // Create a Hadoop Job
		Job job = Job.getInstance(conf, "Hashtag Counts");
        
        // Attach the job to this Class
		job.setJarByClass(TopHashtags.class); 
        
        // Number of reducers
		job.setNumReduceTasks(reduce_tasks);
        
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(IntWritable.class);
        
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
		// a MapReduce job to sort by descending value in int
		new SortByValueDescending().sortByValueDescendingInt(conf, TopHashtags.class, new Path(otherArgs[1]), new Path(otherArgs[2]));
	} 

	
    // The Map Class
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>  {
		
        // The map method 
		public void map(LongWritable key, Text value, Context context)
							throws IOException, InterruptedException  {
			HashSet<String> hashtags = new HashSet<String>();
			//Gson gson = new Gson();
			//JsonObject jobj = gson.fromJson(value.toString(), JsonObject.class).getAsJsonObject("entities");
			//JsonArray jarr = jobj.getAsJsonArray("hashtags");
			/*JsonParser parser = new JsonParser();
			JsonObject jo =(JsonObject) parser.parse(value.toString());
			JsonObject jobj = jo.getAsJsonObject("entities");
			JsonArray jarr = (JsonArray) jobj.getAsJsonArray("hashtags");*/
			
			//for(int i = 0; i < jarr.size(); i ++)
			//	hashtags.add(jarr.get(i).getAsJsonObject().get("text").getAsString());
			try {
				JSONParser parser = new JSONParser();
				JSONObject jobj;
				
					jobj = (JSONObject) parser.parse(value.toString());	// parse one record from JSON
				
				JSONObject entities = (JSONObject) jobj.get("entities");	// find the entities object in JSON
				JSONArray hash = (JSONArray) entities.get("hashtags");	// retrieve the hashtags as array
				for(int i = 0; i < hash.size(); i ++) {	// retrieve all hashtag text
					JSONObject obj = (JSONObject) hash.get(i);
					
					hashtags.add(obj.get("text").toString());
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for(String hashtag : hashtags)	// emit (hashtag, 1) 	
				context.write(new Text(hashtag), new IntWritable(1));
		} 	
	} 
	
    // The Reduce class
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>  {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {
			int sum = 0;
			for (IntWritable val : values) {	// sum the occurrence for hashtags
				sum += val.get();
				//context.write(key, values);
			}
			context.write(key, new IntWritable(sum));	// output (hashtag, count)
		} 
	}
	
	

}
