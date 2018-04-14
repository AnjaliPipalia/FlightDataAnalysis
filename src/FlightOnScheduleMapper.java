import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightOnScheduleMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 String line = value.toString();
	        String[] lineSplit = line.split(",");
	        // value = { PI, 23}
	        if(lineSplit[8]!="NA" && lineSplit[14]!="NA"){
	        	context.write(new Text(lineSplit[8]), new IntWritable(Integer.parseInt(lineSplit[14])));
	        }
	}
}
