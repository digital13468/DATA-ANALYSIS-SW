import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

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


public class TweetersHashtag {

public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
	    
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
	    int reduce_tasks = 2;
		
        // Create a Hadoop Job
		Job job = Job.getInstance(conf, "Tweeter's Tweets Count and Hashtag Used Most");
        
        // Attach the job to this Class
		job.setJarByClass(TweetersHashtag.class); 
        
        // Number of reducers
		job.setNumReduceTasks(reduce_tasks);
        
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
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
		// sort by tweets count
		new SortByValueDescending().sortByValueDescendingInt(conf, TweetersHashtag.class, new Path(otherArgs[1]), new Path(otherArgs[2]));
	} 

	
    // The Map Class
	public static class Map extends Mapper<LongWritable, Text, Text, Text>  {
		
        // The map method 
		public void map(LongWritable key, Text value, Context context)
							throws IOException, InterruptedException  {
			
			//context.write(new Text(key.toString()), value);
			JSONParser parser = new JSONParser();
			JSONObject jobj;
			
			try {
				jobj = (JSONObject) parser.parse(value.toString());	// parse the returned JSON record
				
				JSONObject user = (JSONObject) jobj.get("user");	// find user field
				String screenName = user.get("screen_name").toString();	// find the screen_name
				
				String outputValue = "tweets_count: 1, hashtags: ";	// format the output value
				
				HashSet<String> hashtags = new HashSet<String>();	// distinct hashtags from one record
				JSONObject entities = (JSONObject) jobj.get("entities");	// find entities field
				JSONArray hash = (JSONArray) entities.get("hashtags");	// take all hashtags used
				for(int i = 0; i < hash.size(); i ++) {
					JSONObject obj = (JSONObject) hash.get(i);
					hashtags.add(obj.get("text").toString());
				}
				for(String hashtag : hashtags)	// put hashtags together 
					outputValue = outputValue + hashtag + ", ";
				context.write(new Text(screenName), new Text(outputValue));

				//context.write(new Text(screenName), new Text("1"));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		} 	
	} 
	
    // The Reduce class
	public static class Reduce extends Reducer<Text, Text, Text, IntWritable>  {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
			int count = 0;
			HashMap<String, Integer> hashtagsCount = new HashMap<String, Integer>();
			for (Text val : values) {
				String valString= val.toString();
				count += Integer.parseInt(valString.substring(valString.indexOf("tweets_count: ") + "tweets_count: ".length(), valString.indexOf(", hashtags: ")));
				
				String hashtags = valString.substring(valString.indexOf(", hashtags: ") + ", hashtags: ".length());
				int stringEnd = hashtags.lastIndexOf(", ");
				int stringInd = 0;
				while(stringInd < stringEnd) {
					String hashtag = hashtags.substring(stringInd, hashtags.indexOf(", ", stringInd));
					stringInd = hashtags.indexOf(", ", stringInd) + ", ".length();
					if(hashtagsCount.containsKey(hashtag))
						hashtagsCount.put(hashtag, hashtagsCount.get(hashtag) + 1);
					else
						hashtagsCount.put(hashtag, 1);
				}
			}
			List<Entry<String, Integer>> listData = new ArrayList<Entry<String, Integer>>(hashtagsCount.entrySet());
			Collections.sort(listData, new Comparator<Entry<String, Integer>>()
				{
					public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2){
						return (o2.getValue()).compareTo(o1.getValue());
					}
				}
			);
	
			//context.write(new Text(key.toString() + Arrays.toString(listData.toArray()).replace(", ", ",")), new IntWritable(count));
			context.write(new Text(key.toString() + "[" + listData.get(0).toString() + "]"), new IntWritable(count));
		} 
	}
}
