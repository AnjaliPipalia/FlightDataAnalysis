import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightOnScheduleReducer extends Reducer<Text, IntWritable, Text, Text>{
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
	}
	
}
