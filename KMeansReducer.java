package pkg;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pkg.KMeansDriver.PROGRAM_COUNTERS;

public class KMeansReducer extends Reducer<Text, Text, Text, Text> {
	static ArrayList<String> cents = new ArrayList<String>();
	static double distanceBeteenCentroidsMin = 0;
	static double distanceBeteenCentroidsMax = 0;

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		System.out.println("Reducer Start");
		context.getCounter(PROGRAM_COUNTERS.MORE_ITERATIONS).setValue(1);
		HashMap<Integer, Double> dataVarsSum = new HashMap<Integer, Double>();
		int numberOfValues = 0; // Number of data points
		int numberOfVars = 0; // Numver of variables for each data point

		// Loop through the data, calculate sum of all the data points, for each
		// variable and save the sum in the hashmap
		for (Text value : values) {
			numberOfValues++;
			System.out.println("Key value : " + key.toString());
			System.out.println("Value of value : " + value.toString());
			System.out.println("In first for loop");
			String[] dataVars = value.toString().split(",");
			for (int i = 0; i < dataVars.length; i++) {
				numberOfVars = dataVars.length;
				System.out.println("In second for loop");
				System.out.println("dataVars.length : " + dataVars.length);
				if (numberOfValues == 1) {
					System.out.println("In if condition");
					dataVarsSum.put(i,
							Double.parseDouble(dataVars[i].toString()));
				} else {
					System.out.println("In else condition");
					dataVarsSum
							.put(i,
									dataVarsSum.get(i)
											+ Double.parseDouble(dataVars[i]
													.toString()));
				}
			}
			context.write(key, value);
		}
		System.out.println("numberOfValues : " + numberOfValues);
		System.out.println("numberOfVars : " + numberOfVars);
		// Divide each of the variable sum with total number of variables to get
		// the average
		for (int i = 0; i < numberOfVars; i++) {
			dataVarsSum.put(i, (dataVarsSum.get(i) / numberOfValues));
			System.out.println("Final value : " + i + " : "
					+ dataVarsSum.get(i));
		}
		// Append all the variables to create coordinates for new centroid
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < numberOfVars; i++) {
			if (i != 0) {
				sb.append("," + dataVarsSum.get(i));
			} else {
				sb.append(dataVarsSum.get(i));
			}
		}
		System.out
				.println("Final centroid coordinates output, after calculating average: "
						+ sb.toString());

		// Before calculating the distance between old and new centroids, we
		// have to re-populate the Centroids class file with the centroid
		// coordinates, since they would have been lost
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

		FileSystem fs = FileSystem.get(context.getConfiguration());
		int iterationNumber = Integer.parseInt(context.getConfiguration().get(
				"Iteration_Number"));
		double convergence = Integer.parseInt(context.getConfiguration().get(
				"Argument2"));
		String argument0 = context.getConfiguration().get("Argument0");
		String argument1 = context.getConfiguration().get("Argument1");
		System.out.println("Iteration number : " + iterationNumber);
		System.out.println("Argument0 : " + argument0);
		System.out.println("Argument1 : " + argument1);

		// Add the new centroid coordinates to the ArrayList
		cents.add(key.toString() + "\t" + sb.toString());

		// Calculate distance between centroids
		double distanceBeteenCentroids = Centroids.distanceBeteenCentroids(
				key.toString(), sb.toString());
		System.out.println("Distance between centroids : "
				+ distanceBeteenCentroids);

		// Write the new centroid information to the file that can will be
		// stored in cache for the next iteration
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				fs.create(new Path(argument0 + "_" + iterationNumber
						+ "/Centroids.txt"), true)));
		for (int i = 0; i < cents.size(); i++) {
			System.out
					.println("Iterating in for loop and writing centroid data to the file");
			bw.write(cents.get(i));
			bw.write("\n");
		}
		bw.close();

		// Always store the minimum value of the centroid distances in the
		// variable
		if (iterationNumber == 1) {
			distanceBeteenCentroidsMin = distanceBeteenCentroids;
			distanceBeteenCentroidsMax = distanceBeteenCentroids;
		} else {
			distanceBeteenCentroidsMax = Math.max(distanceBeteenCentroids,
					distanceBeteenCentroidsMax);
		}
		// Stop the iteration only when the distance between centroids is
		// less than the pre-determined number
		if (distanceBeteenCentroidsMax <= convergence) {
			System.out.println("In final ELSE IF");
			context.getCounter(PROGRAM_COUNTERS.MORE_ITERATIONS).setValue(0);
		}
		System.out.println("Reducer End");
	}
}