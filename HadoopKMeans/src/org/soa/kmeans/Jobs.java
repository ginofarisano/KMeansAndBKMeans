package org.soa.kmeans;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.soa.cenrandom.CombinerCentroids;
import org.soa.cenrandom.MapCentroids;
import org.soa.cenrandom.ReducerCentroids;

public class Jobs extends Configured implements Tool {
	
	Path finalResult = new Path("RESULT/");
	
	Path outputPath, inputPath;
	
	int numbersOfDocuments, numberOfCentroids;
	FileSystem fs;
	
	private static String pathCentroid = "CENTROIDS/";
	
	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Jobs(), args);
		System.exit(res);
	}
	
	/**
	 *  example of arguments: INPUT 5 5 250
	 *  -INPUT is a folder input
	 *  -the second argument is the number of centroids
	 *  -the third argument is the number of iteration. N.B if there are convercence
	 *   are lower
	 *  -fourth are the number of document
	 *  
	 */
	
	public int run(String[] args) throws Exception {
		Configuration conf;
		this.fs = FileSystem.get(this.getConf());
		this.outputPath = new Path("OUTPUT");
		this.inputPath = new Path(args[0]);
		this.numberOfCentroids=Integer.parseInt(args[1]);
		int numberOfIterations=Integer.parseInt(args[2]);
		//number of document in input
		this.numbersOfDocuments = Integer.parseInt(args[3]);
		int succesFlag=1;
		Path t = new Path(pathCentroid);
		if(fs.exists(t))
			fs.delete(t, true);
		
		//generate a random centroids
		if(generateCentroids()==1)
			System.out.println("Problem in gerating centroid");
		
		int iterationForPrintResult = 0;
		
		for(int i=0;i<numberOfIterations-1;i++){
			
			iterationForPrintResult=i;
			
			conf = getConf();
			Path outPath = outputPath;
			if(fs.exists(outPath))
				fs.delete(outPath, true);
			Job job = Job.getInstance(conf, "KMeans iteration: "+i);
			FileInputFormat.setInputPaths(job, inputPath);
			FileOutputFormat.setOutputPath(job, outPath);
			
			//set the distribuited cache
			job.addCacheFile(new URI(pathCentroid + i ));  // new Path().toUri();
			job.setJarByClass(this.getClass());
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(DocumentWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(MapWritable.class);
			job.setMapperClass(MapKMeans.class);
			job.setCombinerClass(CombineKMeans.class);
			job.setReducerClass(ReduceKMeans.class);
			succesFlag=job.waitForCompletion(true) ? 0 : 1;
			
			if(succesFlag==1){
				System.out.println("Problem in iteration "+i);
				return 1;
			}
			Path newCentroid = new Path(pathCentroid +  (i+1));
			FileUtil.copyMerge(fs, outPath, fs, newCentroid, false, this.getConf(), null);
			if(checkConvergence(i))
				break;
		}
		
		printResult(iterationForPrintResult);
		
		return 0;
	}
	
	private int printResult(int iterationForPrintResult) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();;
		Path outPath = outputPath;
		if(fs.exists(outPath))
			fs.delete(outPath, true);
		Job job = Job.getInstance(conf, "KMeans print result: ");
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPath);
		
		//set the distribuited cache
		job.addCacheFile(new URI(pathCentroid + iterationForPrintResult ));  // new Path().toUri();
		job.setJarByClass(this.getClass());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DocumentWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MapKMeans.class);
		job.setCombinerClass(CombineKMeans.class);
		job.setReducerClass(ReduceKMeansForPrintResult.class);
		int succesFlag=job.waitForCompletion(true) ? 0 : 1;
		
		if(succesFlag==1){
			System.out.println("Problem in print result ");
			return 1;
		}
		
		FileUtil.copyMerge(fs, outPath, fs, finalResult, false, this.getConf(), null);
		
		return 0;
		
	}

	/**
	 * see if the centroids calculates are the same of the previous
	 */
	private boolean checkConvergence(int numberOfIterations) throws IllegalArgumentException, IOException {
		
		ArrayList <MapWritable> c1 = new ArrayList<MapWritable>();
		ArrayList <MapWritable> c2 = new ArrayList<MapWritable>();
		
		Configuration conf=new Configuration();
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(new Path(pathCentroid +  numberOfIterations)));
		Text key = new Text();
		MapWritable value;
		MapWritable val=new MapWritable();
		while (reader.next(key, val)) {
			value=new MapWritable(val);
			c1.add(value);     
		}
		reader.close();
		reader = new SequenceFile.Reader(conf, Reader.file(new Path(pathCentroid + (numberOfIterations+1))));
		while (reader.next(key, val)) {
			value=new MapWritable(val);
			c2.add(value);     
		}
		reader.close();
		if(c1.equals(c2))
			return true;
		else return false;
	}
	private int generateCentroids() throws Exception {
		
		
		//to remove
		//write();
		
		Configuration conf = new Configuration();
		//delete the older output dir
		if(fs.exists(outputPath))
			fs.delete(outputPath, true);
		
		conf.setInt("centroids", this.numberOfCentroids);
		
		
		conf.setInt("prob", (this.numbersOfDocuments / this.numberOfCentroids) );
		
		Job jobCentroids = Job.getInstance(conf,"Calculate random centroids");
		FileInputFormat.setInputPaths(jobCentroids, this.inputPath);
		FileOutputFormat.setOutputPath(jobCentroids, this.outputPath);
		jobCentroids.setJarByClass(this.getClass());
		jobCentroids.setInputFormatClass(SequenceFileInputFormat.class);
		jobCentroids.setOutputFormatClass(SequenceFileOutputFormat.class);
		jobCentroids.setMapOutputKeyClass(IntWritable.class);
		jobCentroids.setMapOutputValueClass(MapWritable.class);
		jobCentroids.setMapperClass(MapCentroids.class);
		jobCentroids.setCombinerClass(CombinerCentroids.class);
		jobCentroids.setReducerClass(ReducerCentroids.class);
		jobCentroids.setOutputKeyClass(Text.class);
		jobCentroids.setOutputValueClass(MapWritable.class);
		int result=jobCentroids.waitForCompletion(true) ? 0 : 1;
		//merge the files of the map/reduce 
		FileUtil.copyMerge(this.fs, this.outputPath, this.fs, new Path(pathCentroid + "0"  ), false, this.getConf(), null);
		
		return result;
	
	}

}
