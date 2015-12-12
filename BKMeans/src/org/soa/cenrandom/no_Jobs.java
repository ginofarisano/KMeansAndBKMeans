package org.soa.cenrandom;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;


public class no_Jobs extends Configured implements Tool {


	public static void main(String args[]) throws Exception {
		
		int res = ToolRunner.run(new no_Jobs(), args);
		System.exit(res);
	}


	public int run(String[] args) throws Exception {
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		
		Configuration conf = new Configuration();
		conf.setInt("index", -1);
		Job jobCentroids = Job.getInstance(conf,"Calculate centroids");
		FileInputFormat.setInputPaths(jobCentroids, inputPath);
		FileOutputFormat.setOutputPath(jobCentroids, outputPath);
		jobCentroids.setJarByClass(this.getClass());
		
		jobCentroids.setInputFormatClass(SequenceFileInputFormat.class);
		
		//jobCentroids.setInputFormatClass(TextInputFormat.class);
		jobCentroids.setOutputFormatClass(TextOutputFormat.class);
		jobCentroids.setMapOutputKeyClass(LongWritable.class);
		jobCentroids.setMapOutputValueClass(Text.class);
		
		jobCentroids.setMapperClass(MapCentroids.class);
		jobCentroids.setCombinerClass(CombinerCentroids.class);
		jobCentroids.setReducerClass(ReducerCentroids.class);
		
		jobCentroids.setMapOutputKeyClass(IntWritable.class);
		jobCentroids.setMapOutputValueClass(Text.class);
		jobCentroids.setOutputKeyClass(Text.class);
		jobCentroids.setOutputValueClass(Text.class);
		
		//job.addCacheFile(new URI("CENTROIDS/centroid"+i+".txt"));
		
		jobCentroids.waitForCompletion(true);
		

		return 0;
	}
}






