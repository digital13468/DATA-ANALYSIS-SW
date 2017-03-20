import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

// Create a custom Record Locator
public class JsonRecordReader extends RecordReader<LongWritable, Text> {
                                                    
    private LineReader lineReader;
    private LongWritable key;
    private Text value;
    long start, end, position, no_of_calls;
                                                    
    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        // Retrieve configuration
        Configuration conf = context.getConfiguration();
        // This InputSplit is a FileInputSplit
        FileSplit split = (FileSplit) genericSplit;
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(conf);
        // starting from "start" and "end" positions
        start = split.getStart();
        end = start + split.getLength();
        position = start;
        // Retrieve file
        FSDataInputStream input_file = fs.open(split.getPath());
        input_file.seek(start);
                                                            
        lineReader = new LineReader(input_file, conf); 
        // line number in the file
        no_of_calls = 0;
    }  
         
    @Override
    public float getProgress() throws IOException, InterruptedException {
        
        if (start == end) {
            
            return 0.0f;
        }
        else {
            
            return Math.min(1.0f, (position - start) / (float)(end - start));
        }
    }
                                                    
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
                                                        
        return key;
    }
                                                    
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
                                                        
        return value;
    }
                                                    
    @Override                                                
    public boolean nextKeyValue() throws IOException {
    	// line number
        no_of_calls = no_of_calls + 1;
        
        if (position >= end)  {
            
            return false;
        }
                                                        
        if (key == null) {
            
            key = new LongWritable();
        }
        
        if (value == null) {
            
            value = new Text(" ");
        }
        Text temp_text = new Text(" ");	// store the read-in content    
        int read_length = lineReader.readLine(temp_text);	// track the byte position
        position = position + read_length;	// change the position
        String temp = temp_text.toString();
        while(position < end && !temp_text.toString().startsWith("{")) {	// find the first {
        	no_of_calls = no_of_calls + 1;
            read_length = lineReader.readLine(temp_text);
            position = position + read_length;
           
        }
        if(temp_text.toString().startsWith("{")) {	// find the starting {
        	temp = temp_text.toString();	
        	
        	key.set(no_of_calls);	// move line

            no_of_calls = no_of_calls + 1;	// move position
            read_length = lineReader.readLine(temp_text);	
            position = position + read_length;
            temp = temp + temp_text.toString();
            while(!temp_text.toString().startsWith("}") ) {	// accumulate content
//               	for(int i = 0; i < 2; i++) {
        		no_of_calls = no_of_calls + 1;
        		read_length = lineReader.readLine(temp_text);
	            position = position + read_length;
            
	            temp = temp + temp_text.toString();
	            
	            
        	}	// find the ending {
            if(temp_text.toString().startsWith("},")) 
            	value.set(temp.substring(0, temp.length() - 1));
            else if(temp_text.toString().startsWith("}"))
            	value.set(temp);
            else	// incomplete 
            	return false;
        }
        else	// no more JSON object
        	return false;
        return true;
    }
          
    @Override
    public void close() throws IOException {
              
        if ( lineReader != null )
            lineReader.close();
    }
    
}