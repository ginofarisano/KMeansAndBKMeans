package org.soa.kmeans;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.soa.cenrandom.MapCentroids;
import org.soa.cenrandom.ReducerCentroids;

public class Jobs
  extends Configured
  implements Tool
{
  Path finalResult = new Path("RESULT/");
  Path outputPath;
  Path inputPath;
  int numbersOfDocuments;
  int numberOfCentroids;
  int distance;
  int numberOfMachine;
  FileSystem fs;
  private static String pathCentroid = "CENTROIDS/";
  
  public static void main(String[] args)
    throws Exception
  {
    int res = ToolRunner.run(new Jobs(), args);
    System.exit(res);
  }
  
  public int run(String[] args)
    throws Exception
  {
    this.fs = FileSystem.get(getConf());
    this.outputPath = new Path("OUTPUT");
    this.inputPath = new Path(args[0]);
    this.numberOfCentroids = Integer.parseInt(args[1]);
    int numberOfIterations = Integer.parseInt(args[2]);
    
    this.numbersOfDocuments = Integer.parseInt(args[3]);
    this.distance = Integer.parseInt(args[4]);
    
    this.numberOfMachine = Integer.parseInt(args[5]);
    
    int succesFlag = 1;
    Path t = new Path(pathCentroid);
    if (this.fs.exists(t)) {
      this.fs.delete(t, true);
    }
    if (generateCentroids() == 1)
    {
      System.out.println("Problem in gerating centroid");
      return 1;
    }
    int iterationForPrintResult = 0;
    for (int i = 0; i < numberOfIterations; i++)
    {
      iterationForPrintResult = i;
      
      Configuration conf = new Configuration();
      
      Path outPath = this.outputPath;
      if (this.fs.exists(outPath)) {
        this.fs.delete(outPath, true);
      }
      Job job = Job.getInstance(conf, "KMeans iteration: " + i);
      FileInputFormat.setInputPaths(job, new Path[] { this.inputPath });
      FileOutputFormat.setOutputPath(job, outPath);
      
      job.addCacheFile(new URI(pathCentroid + i));
      job.setJarByClass(getClass());
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(DocumentWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(MapWritable.class);
      job.setMapperClass(MapKMeans.class);
      job.setCombinerClass(CombineKMeans.class);
      job.setReducerClass(ReduceKMeans.class);
      job.setNumReduceTasks(this.numberOfMachine / 2);
      
      conf.setInt("distance", this.distance);
      
      conf.setInt("topX", 200);
      
      succesFlag = job.waitForCompletion(true) ? 0 : 1;
      if (succesFlag == 1)
      {
        System.out.println("Problem in iteration " + i);
        return 1;
      }
      Path newCentroid = new Path(pathCentroid + (i + 1));
      FileUtil.copyMerge(this.fs, outPath, this.fs, newCentroid, false, getConf(), null);
    }
    return 0;
  }
  
  private int printResult(int iterationForPrintResult)
    throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException
  {
    Configuration conf = new Configuration();
    Path outPath = this.outputPath;
    if (this.fs.exists(outPath)) {
      this.fs.delete(outPath, true);
    }
    Job job = Job.getInstance(conf, "KMeans print result: ");
    FileInputFormat.setInputPaths(job, new Path[] { this.inputPath });
    FileOutputFormat.setOutputPath(job, outPath);
    
    job.addCacheFile(new URI(pathCentroid + iterationForPrintResult));
    job.setJarByClass(getClass());
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(DocumentWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(MapKMeans.class);
    job.setCombinerClass(CombineKMeans.class);
    job.setReducerClass(ReduceKMeansForPrintResult.class);
    int succesFlag = job.waitForCompletion(true) ? 0 : 1;
    if (succesFlag == 1)
    {
      System.out.println("Problem in print result ");
      return 1;
    }
    FileUtil.copyMerge(this.fs, outPath, this.fs, this.finalResult, false, getConf(), null);
    
    return 0;
  }
  
  private boolean checkConvergence(int numberOfIterations)
    throws IllegalArgumentException, IOException
  {
    ArrayList<MapWritable> c1 = new ArrayList();
    ArrayList<MapWritable> c2 = new ArrayList();
    
    Configuration conf = new Configuration();
    SequenceFile.Reader reader = new SequenceFile.Reader(conf, new SequenceFile.Reader.Option[] { SequenceFile.Reader.file(new Path(pathCentroid + numberOfIterations)) });
    Text key = new Text();
    
    MapWritable val = new MapWritable();
    while (reader.next(key, val))
    {
      MapWritable value = new MapWritable(val);
      c1.add(value);
    }
    reader.close();
    reader = new SequenceFile.Reader(conf, new SequenceFile.Reader.Option[] { SequenceFile.Reader.file(new Path(pathCentroid + (numberOfIterations + 1))) });
    while (reader.next(key, val))
    {
      MapWritable value = new MapWritable(val);
      c2.add(value);
    }
    reader.close();
    if (c1.equals(c2)) {
      return true;
    }
    return false;
  }
  
  private int generateCentroids()
    throws Exception
  {
    Configuration conf = new Configuration();
    if (this.fs.exists(this.outputPath)) {
      this.fs.delete(this.outputPath, true);
    }
    conf.setInt("centroids", this.numberOfCentroids);
    
    conf.setInt("prob", this.numbersOfDocuments / this.numberOfCentroids);
    
    Job jobCentroids = Job.getInstance(conf, "Calculate random centroids");
    FileInputFormat.setInputPaths(jobCentroids, new Path[] { this.inputPath });
    FileOutputFormat.setOutputPath(jobCentroids, this.outputPath);
    jobCentroids.setJarByClass(getClass());
    
    jobCentroids.setInputFormatClass(SequenceFileInputFormat.class);
    jobCentroids.setOutputFormatClass(TextOutputFormat.class);
    
    jobCentroids.setMapOutputKeyClass(IntWritable.class);
    jobCentroids.setMapOutputValueClass(MapWritable.class);
    
    jobCentroids.setMapperClass(MapCentroids.class);
    
    jobCentroids.setReducerClass(ReducerCentroids.class);
    
    jobCentroids.setOutputKeyClass(Text.class);
    jobCentroids.setOutputValueClass(Text.class);
    
    jobCentroids.setNumReduceTasks(1);
    
    int result = jobCentroids.waitForCompletion(true) ? 0 : 1;
    if (result == 0) {
      FileUtil.copyMerge(this.fs, this.outputPath, this.fs, new Path(pathCentroid + "0"), false, getConf(), null);
    }
    return result;
  }
}
