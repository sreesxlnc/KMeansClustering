package pkg;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReadCentroidsMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		System.out.println("Mapper Start");
/*
		String temp[] = value.toString().split("\\t");
		System.out.println("Statement1");
		System.out.println("Key after split : " + temp[0]);
		System.out.println("Value after split : " + temp[1]);
		System.out.println("Read from centroid class: ");
		System.out.println("Key:Value --> " + "C1" + ":"
				+ Centroids.centroids.get("C1"));
		System.out.println("Key:Value --> " + "C2" + ":"
				+ Centroids.centroids.get("C2"));
		// Save centroid in HashMap
		if (temp[0].substring(0, 1).equalsIgnoreCase("c")) {
			Centroids.centroids.put(temp[0], temp[1]);
			System.out.println("Print from HashMap :");
			System.out.println("Key:Value --> " + temp[0] + ":"
					+ Centroids.centroids.get(temp[0]));
			context.write(new Text(temp[0]), new Text(temp[1]));
			System.out.println("Mapper End");
		} else {
			context.write(new Text(temp[0]), new Text(temp[1]));
		}*/
	}
}