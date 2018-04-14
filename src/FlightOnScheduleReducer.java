import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightOnScheduleReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	public TreeSet<OnScheduledFlightWithHighestProbability> top3 = new TreeSet<>();
	public TreeSet<OnScheduledFlightWithLowestProbability> bottom3 = new TreeSet<>();

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
		double probability = (double) (numberOfOnScheduledFlight / count);
		top3.add(new OnScheduledFlightWithHighestProbability(key, probability));

		if (top3.size() > 3) {
			top3.pollLast();
		}
		bottom3.add(new OnScheduledFlightWithLowestProbability(key, probability));

		if (bottom3.size() > 3) {
			bottom3.pollLast();
		}

	}

	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(new Text("highest probability for being on schedule"), null);
		for (OnScheduledFlightWithHighestProbability key : top3) {
			context.write(key.uniqueID, new DoubleWritable(key.probability));
		}
		context.write(new Text("lowest probability for being on schedule"), null);
		for (OnScheduledFlightWithLowestProbability key : bottom3) {
			context.write(key.uniqueID, new DoubleWritable(key.probability));
		}
	}

	public class OnScheduledFlightWithHighestProbability
			implements Comparable<OnScheduledFlightWithHighestProbability> {
		double probability;
		Text uniqueID;

		OnScheduledFlightWithHighestProbability(Text uniqueID, double probability) {
			this.probability = probability;
			this.uniqueID = uniqueID;
		}

		@Override
		public int compareTo(OnScheduledFlightWithHighestProbability onScheduledFlightWithHighestProbability) {

			if (this.probability < onScheduledFlightWithHighestProbability.probability) {
				return 1;
			} else if (this.probability == onScheduledFlightWithHighestProbability.probability) {
				return 0;
			} else
				return -1;

		}
	}

	public class OnScheduledFlightWithLowestProbability implements Comparable<OnScheduledFlightWithLowestProbability> {
		double probability;
		Text uniqueID;

		OnScheduledFlightWithLowestProbability(Text uniqueID, double probability) {
			this.probability = probability;
			this.uniqueID = uniqueID;
		}

		@Override
		public int compareTo(OnScheduledFlightWithLowestProbability onScheduledFlightWithLowestProbability) {

			if (this.probability > onScheduledFlightWithLowestProbability.probability) {
				return 1;
			} else if (this.probability == onScheduledFlightWithLowestProbability.probability) {
				return 0;
			} else
				return -1;

		}
	}

}
