import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


// Create a Custom Input Format
// Subclass from FileInputFormat class that provides 
// much of the basic handling necessary to manipulate files.
public class JsonInputFormat extends FileInputFormat<LongWritable, Text> {
    @Override 
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
                            
        return new JsonRecordReader();
    }
}
    
