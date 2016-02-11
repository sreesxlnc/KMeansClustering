package pkg;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class Centroids {
	private static HashMap<String, String> centroids = new HashMap<String, String>();

	// Take key, value and insert into hashmap
	public static void insertCentroid(String value) {
		System.out.println("Inside insertCentroid");
		String temp[] = value.toString().split("\\t");
		centroids.put(temp[0], temp[1]);
		System.out.println("Inside insertCentroid. Centroid Values: " + temp[0]
				+ ":" + centroids.get(temp[0]));
	}

	// Return value by taking key
	public static String getCentroid(String key) {
		return centroids.get(key);
	}

	// This method takes the co-ordinates of a point as string and returns the
	// nearest centroid ID as a String
	public static String calculateNearestCentroid(String value) {
		System.out.println("calculateNearestCentroid Start");
		// Split the string to get data point coordinates
		String[] dataVars = value.split(",");
		System.out.println("Afte splitting the input string");
		// Tree map
		TreeMap<Double, String> tm = new TreeMap<Double, String>();
		System.out.println("Before iterator declaration");
		Iterator it = centroids.entrySet().iterator();
		System.out.println("After iterator declaration");
		while (it.hasNext()) {
			System.out.println("While loop start");
			Map.Entry pair = (Map.Entry) it.next();
			// Split the string to get centroid coordinates
			String[] centroidVars = pair.getValue().toString().split(",");
			double sum = 0;
			double distance;
			for (int i = 0; i < centroidVars.length; i++) {
				System.out.println("In for loop");
				sum = sum
						+ Math.pow((Double.parseDouble(dataVars[i]) - Double
								.parseDouble(centroidVars[i])), 2);
				System.out.println("Sum : " + sum);
				if (i == centroidVars.length - 1) {
					distance = Math.sqrt(sum);
					System.out.println("Distance from centroid: "
							+ pair.getKey().toString() + " : " + distance);
					tm.put(distance, pair.getKey().toString());
				}
			}
		}
		Iterator tmit = tm.entrySet().iterator();
		while (tmit.hasNext()) {
			System.out.println("While loop of TreeMap");
			Map.Entry tmpair = (Map.Entry) tmit.next();
			System.out.println("Minimum distance : "
					+ tmpair.getKey().toString());
			System.out.println("Closest centroid : "
					+ tmpair.getValue().toString());
			return tmpair.getValue().toString();
		}

		return null;
	}

	public static double distanceBeteenCentroids(String key, String value) {
		System.out
				.println("In distanceBeteenCentroids method of Centroids class");
		System.out.println("Key : " + key);
		System.out.println("Value : " + value);
		System.out.println("Statement1");
		String oldCentroid = Centroids.centroids.get(key);
		System.out.println("OldCentroid : " + oldCentroid);
		System.out.println("Statement2");
		String[] oldCoords = oldCentroid.split(",");
		System.out.println("Statement3");
		String newCentroid = value;
		System.out.println("New Centroid : " + value);
		System.out.println("Statement4");
		String[] newCoords = value.split(",");
		System.out.println("Statement5");
		double sum = 0;
		double distance;
		for (int i = 0; i < oldCoords.length; i++) {
			System.out.println("In for loop of distanceBeteenCentroids method");
			sum = sum
					+ Math.pow((Double.parseDouble(oldCoords[i]) - Double
							.parseDouble(newCoords[i])), 2);
		}
		return distance = Math.sqrt(sum);
	}
	/*
	 * public static void main(String args[]) throws Exception { //
	 * centroids.put("C1", "1,2"); // centroids.put("C2", "9,10"); String s =
	 * calculateNearestCentroid("1,6"); System.out.println("Nearest centroid : "
	 * + s); }
	 */
}