package pkg;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KMeansPartitioner extends Partitioner<Text, Text> {
	static int i = 0;

	public int getPartition(Text key, Text values, int numReduceTasks) {
		System.out.println("Partitioner start");
		int centroidID = Integer.parseInt(key.toString().substring(1, 2));
		if (centroidID <= (numReduceTasks - 1)) {
			System.out.println("Partitioner : " + centroidID);
			return i;
		}

		else {
			for (int i = 0; i < numReduceTasks; i++) {
				if (centroidID % numReduceTasks == i) {
					if (i == 0) {
						System.out.println("Partitioner : "
								+ (numReduceTasks - 1));
					} else {
						System.out.println("Partitioner : " + (i - 1));
					}
				}
			}
			// Return first partitioner if no condition is satisfied
			return 0;
		}
	}
}
