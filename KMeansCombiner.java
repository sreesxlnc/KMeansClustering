package pkg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansCombiner extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("Combiner start");
		System.out.println("Value printed form Centroids class : "
				+ Centroids.getCentroid("C1").toString());
		System.out.println("Value printed form Centroids class : "
				+ Centroids.getCentroid("C1"));
		for (Text value : values) {
			if (value.toString().substring(0, 1).equalsIgnoreCase("c")) {
				continue;
			} else {
				// Get nearest centroid ID
				String nearCentID = Centroids.calculateNearestCentroid(value
						.toString());
				// Assign the coordinates the above determined centroid and
				// write to the output
				context.write(new Text(nearCentID), value);
			}
		}
		System.out.println("Combiner end");
	}
}