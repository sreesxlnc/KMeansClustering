package pkg;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KMeansDriver extends Configured implements Tool {
	// Declaring a counter to track the iteration condition
	public static enum PROGRAM_COUNTERS {
		MORE_ITERATIONS, ITERATION_NUMBER
	};

	public int run(String args[]) throws Exception {
		long iterationNumber = 1;
		// Job to read centroids from file
		Configuration confCentroids = this.getConf();
		confCentroids.set("Iteration_Number", "" + iterationNumber);
		confCentroids.set("Argument0", args[0]);
		confCentroids.set("Argument1", args[1]);
		confCentroids.set("Argument2", args[2]);
		Job jobCentroids = new Job(confCentroids, "KMeans_Iteration1");
		jobCentroids.setJarByClass(KMeansDriver.class);
		jobCentroids.setMapperClass(KMeansMapper.class);
		// jobCentroids.setCombinerClass(KMeansCombiner.class);
		jobCentroids.setPartitionerClass(KMeansPartitioner.class);
		jobCentroids.setReducerClass(KMeansReducer.class);
		jobCentroids.setNumReduceTasks(3);
		DistributedCache.addCacheFile(new URI("input/Centroids.txt"),
				jobCentroids.getConfiguration());
		FileInputFormat.addInputPath(jobCentroids, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobCentroids, new Path(args[1] + "_1"));
		jobCentroids.setOutputKeyClass(Text.class);
		jobCentroids.setOutputValueClass(Text.class);
		jobCentroids.waitForCompletion(true);

		// Looping and running the jobs in chain until the counter
		// MORE_ITERATIONS becomes zero
		while (jobCentroids.getCounters()
				.findCounter(PROGRAM_COUNTERS.MORE_ITERATIONS).getValue() > 0) {
			iterationNumber++;
			System.out.println("Iteration: " + iterationNumber);
			confCentroids = new Configuration();
			confCentroids.set("Iteration_Number", "" + iterationNumber);
			confCentroids.set("Argument0", args[0]);
			confCentroids.set("Argument1", args[1]);
			confCentroids.set("Argument2", args[2]);
			confCentroids.set("iteration_depth", " " + iterationNumber);
			jobCentroids = new Job(confCentroids, "KMeans_Iteration"
					+ iterationNumber);
			jobCentroids.setJarByClass(KMeansDriver.class);
			jobCentroids.setMapperClass(KMeansMapper.class);
			// jobCentroids.setCombinerClass(KMeansCombiner.class);
			jobCentroids.setPartitionerClass(KMeansPartitioner.class);
			jobCentroids.setReducerClass(KMeansReducer.class);
			jobCentroids.setNumReduceTasks(3);
			DistributedCache.addCacheFile(new URI(args[0] + "_"
					+ (iterationNumber - 1) + "/Centroids.txt"),
					jobCentroids.getConfiguration());
			FileInputFormat.addInputPath(jobCentroids, new Path(args[0]));
			FileOutputFormat.setOutputPath(jobCentroids, new Path(args[1] + "_"
					+ iterationNumber));
			jobCentroids.setOutputKeyClass(Text.class);
			jobCentroids.setOutputValueClass(Text.class);
			jobCentroids.waitForCompletion(true);
		}
		return 0;
	}

	public static void main(String args[]) throws Exception {
		if (args.length < 3) {
			System.err
					.println("Please enter the arguments in the order : Input, Output, Covergence");
		}
		int res = ToolRunner.run(new Configuration(), new KMeansDriver(), args);
		System.exit(res);
	}
}
