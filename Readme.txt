***************************** K-Means Clustering ************************************
1) The program requires the user to upload two input documents:
   a) Centroids.txt: Containing the list of initial centroids
   b) Data.txt: containing the list of data points 
2) We need to pass three arguments to the program:
   Argument1 = Input folder name
   Argument2 = Output folder name
   Argument3 = Convergence criteria
3) The initial centroid information is stored in the Hadoop Distributed Cache and is passed from driver to the Mapper/Reducer. 
   During the iterations, reducer will create a new directory on HDFS and stores the centroid information as a text file.
   The next iteration will take this new folder as cache to store the information.
4) If the program runs for 10 iterations, it creates 20 folders, 10 folders storing the centroid information and other 10 folders storing the points and their assigned centroids.
   e.g: if you execute the following command and the program runs for two iterations:
        $hadoop jar ~/Desktop/Assignment2.jar pkg.KMeansDriver input output 0.5
        It creates four folders input1, output1, input2, output2
   	The input folders will store the centroids and output folders will store the points and centroid to which they are assigned
	The final output is stored in the last set of folders.


