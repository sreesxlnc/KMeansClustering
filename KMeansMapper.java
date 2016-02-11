package pkg;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("Mapper Start");
		// Split the input into two strings and save in an array
		// temp[0] stores the datapoint ID and temp[1] will store the
		// coordinates
		String temp[] = value.toString().split("\\t");
		System.out.println("Statement1");
		System.out.println("Key after split : " + temp[0]);
		System.out.println("Value after split : " + temp[1]);

		// Read centroids from cache file and write to the class
		Path[] filePath = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		System.out.println("Printing filePath[0] : " + filePath[0]);
		BufferedReader br;
		for (int i = 0; i < filePath.length; i++) {
			System.out.println("Inside for mapper loop 1");
			String thisLine = null;
			br = new BufferedReader(new FileReader(filePath[i].toString()));
			while ((thisLine = br.readLine()) != null) {
				System.out.println("Inside while loop mapper");
				Centroids.insertCentroid(thisLine);
			}
		}

		if (!(value.toString().substring(0, 1).equalsIgnoreCase("c"))) {
			// Get the nearest centroid by passing the coordinates
			String nearCentID = Centroids.calculateNearestCentroid(temp[1]
					.toString());

			// Write the output
			context.write(new Text(nearCentID), new Text(temp[1]));

			System.out.println("Mapper End");
		}
	}
}