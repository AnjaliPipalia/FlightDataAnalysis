import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightOnScheduleReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// Input to Reducer = (PS,(1,23,3,44,7,19,11,12......));
		int count = 0, tempVal = 0, numberOfOnScheduledFlight = 0;
		for (IntWritable val : values) {
			count++;
			tempVal = val.get();
			if (tempVal < 10) {
				numberOfOnScheduledFlight++;
			}
		}
		Double probability =(double) (numberOfOnScheduledFlight / count);
		context.write(key, new DoubleWritable(probability));

	}

}
