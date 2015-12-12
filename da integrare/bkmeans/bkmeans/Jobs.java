package org.soa.bkmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.soa.cenrandom.CombinerCentroids;
import org.soa.cenrandom.MapCentroids;
import org.soa.cenrandom.ReducerCentroids;

public class Jobs
  extends Configured
  implements Tool
{
  private static final String RANDOM_CENTROIDS_DIR = "RND_CENTROIDS/";
  private static final String RANDOM_CENTROIDS_PATH = "RND_CENTROIDS/random_centroids";
  private static final String CENTROIDS_DIR = "CENTROIDS/";
  private static final String CENTROIDS_PATH = "CENTROIDS/centroids";
  private static final String TEMP_OUTPUT = "OUTPUT_TEMP/";
  private Path inputPath;
  private Path outputPath;
  private FileSystem fs;
  private int randomCentroid_counter = -1;
  private int centroid_counter = -1;
  private int numDocs;
  private int maxCentroidWords;
  private int numberIfMachine;
  
  public static void main(String[] args)
    throws Exception
  {
    int res = ToolRunner.run(new Jobs(), args);
    System.exit(res);
  }
  
  public int run(String[] args)
    throws Exception
  {
    this.inputPath = new Path(args[0]);
    int numberOfCLusters = Integer.parseInt(args[1]);
    int numberOfIterations = Integer.parseInt(args[2]);
    this.numDocs = Integer.parseInt(args[3]);
    this.maxCentroidWords = Integer.parseInt(args[4]);
    this.numberIfMachine = Integer.parseInt(args[5]);
    
    this.fs = FileSystem.get(getConf());
    
    this.outputPath = new Path("OUTPUT_TEMP/");
    if (this.fs.exists(this.outputPath)) {
      this.fs.delete(this.outputPath, true);
    }
    Path t = new Path("RND_CENTROIDS/");
    if (this.fs.exists(t)) {
      this.fs.delete(t, true);
    }
    t = new Path("CENTROIDS/");
    if (this.fs.exists(t)) {
      this.fs.delete(t, true);
    }
    for (int i = 2; i <= numberOfCLusters; i++)
    {
      int centroIndexMax = maxCentroid();
      
      generateNewCentroids(centroIndexMax);
      for (int j = 0; j < numberOfIterations; j++) {
        if (kMeans(centroIndexMax, j) == 1)
        {
          System.out.println("Err");
          break;
        }
      }
      sostituisciCentroide(centroIndexMax);
    }
    return 0;
  }
  
  private int kMeans(int centroIndexMax, int j)
    throws Exception
  {
    Configuration conf = getConf();
    conf.setInt("index", centroIndexMax);
    conf.setInt("maxCentroidWords", this.maxCentroidWords);
    if (this.fs.exists(this.outputPath)) {
      this.fs.delete(this.outputPath, true);
    }
    Job job = Job.getInstance(conf, "BisecKMeans iteration: " + j);
    FileInputFormat.setInputPaths(job, new Path[] { this.inputPath });
    FileOutputFormat.setOutputPath(job, this.outputPath);
    
    job.addCacheFile(new URI("RND_CENTROIDS/random_centroids_" + this.randomCentroid_counter));
    if (centroIndexMax != -1) {
      job.addCacheFile(new URI("CENTROIDS/centroids_" + this.centroid_counter));
    }
    job.setJarByClass(getClass());
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(DocumentWritable.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.setMapperClass(MapKMeans.class);
    job.setCombinerClass(CombineKMeans.class);
    job.setReducerClass(ReduceKMeans.class);
    
    job.setNumReduceTasks(this.numberIfMachine / 2);
    
    int succesFlag = job.waitForCompletion(true) ? 0 : 1;
    
    randomCentroidi_unisciFile();
    if (succesFlag == 1)
    {
      System.out.println("Problem in iteration " + j);
      return 1;
    }
    return succesFlag;
  }
  
  private void randomCentroidi_unisciFile()
    throws IOException
  {
    this.randomCentroid_counter += 1;
    
    FileUtil.copyMerge(this.fs, this.outputPath, this.fs, new Path("RND_CENTROIDS/random_centroids_" + this.randomCentroid_counter), false, getConf(), null);
    if (this.fs.exists(this.outputPath)) {
      this.fs.delete(this.outputPath, true);
    }
  }
  
  private void sostituisciCentroide(int index)
  {
    ArrayList<String> toWrite = new ArrayList();
    try
    {
      FSDataInputStream sr = this.fs.open(new Path("RND_CENTROIDS/random_centroids_" + this.randomCentroid_counter));
      InputStreamReader isr = new InputStreamReader(sr);
      BufferedReader br = new BufferedReader(isr);
      
      toWrite.add(br.readLine());
      toWrite.add(br.readLine());
      
      br.close();
      isr.close();
      sr.close();
      
      Path centroidePath = new Path("CENTROIDS/centroids_" + this.centroid_counter);
      if (this.fs.exists(centroidePath))
      {
        sr = this.fs.open(centroidePath);
        isr = new InputStreamReader(sr);
        br = new BufferedReader(isr);
        
        int i = 0;
        String str;
        while ((str = br.readLine()) != null)
        {
          String str;
          if (i != index) {
            toWrite.add(str);
          }
          i++;
        }
        br.close();
        isr.close();
        sr.close();
      }
      this.centroid_counter += 1;
      FSDataOutputStream sw = this.fs.create(new Path("CENTROIDS/centroids_" + this.centroid_counter), true);
      OutputStreamWriter osw = new OutputStreamWriter(sw);
      BufferedWriter dw = new BufferedWriter(osw);
      for (String str : toWrite) {
        dw.write(str + "\n");
      }
      dw.close();
      osw.close();
      sw.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  private int maxCentroid()
    throws IOException
  {
    Path p = new Path("CENTROIDS/centroids_" + this.centroid_counter);
    if (!this.fs.exists(p)) {
      return -1;
    }
    FSDataInputStream sr = this.fs.open(p);
    BufferedReader br = new BufferedReader(new InputStreamReader(sr));
    
    int max = -1;
    int value = 0;
    int i = 0;
    int index = -1;
    
    String line = br.readLine();
    while (line != null)
    {
      String[] temp = line.split(";");
      value = Integer.parseInt(temp[0].trim());
      if (value > max)
      {
        max = value;
        index = i;
      }
      i++;
      line = br.readLine();
    }
    this.numDocs = value;
    
    br.close();
    sr.close();
    return index;
  }
  
  private boolean checkConvergenza()
    throws IllegalArgumentException, IOException
  {
    ArrayList<String> toWrite = new ArrayList();
    
    FSDataInputStream sr = this.fs.open(new Path("RND_CENTROIDS/random_centroids_" + this.randomCentroid_counter));
    InputStreamReader isr = new InputStreamReader(sr);
    BufferedReader br = new BufferedReader(isr);
    
    toWrite.add(br.readLine());
    toWrite.add(br.readLine());
    
    br.close();
    isr.close();
    sr.close();
    
    sr = this.fs.open(new Path("RND_CENTROIDS/random_centroids_" + (this.randomCentroid_counter - 1)));
    isr = new InputStreamReader(sr);
    br = new BufferedReader(isr);
    
    toWrite.add(br.readLine());
    toWrite.add(br.readLine());
    
    br.close();
    isr.close();
    sr.close();
    
    String v1 = (String)toWrite.get(0);
    String v2 = (String)toWrite.get(2);
    if (v1.equals(v2))
    {
      v1 = (String)toWrite.get(1);
      v2 = (String)toWrite.get(3);
      if (v1.equals(v2)) {
        return true;
      }
    }
    else
    {
      v1 = (String)toWrite.get(1);
      if (v1.equals(v2))
      {
        v1 = (String)toWrite.get(0);
        v2 = (String)toWrite.get(3);
        if (v1.equals(v2)) {
          return true;
        }
      }
    }
    return false;
  }
  
  private void generateNewCentroids(int indexCentroidMax)
    throws Exception
  {
    Configuration conf = new Configuration();
    if (this.fs.exists(this.outputPath)) {
      this.fs.delete(this.outputPath, true);
    }
    conf.setInt("index", indexCentroidMax);
    conf.setInt("prob", this.numDocs / 20);
    Job jobCentroids = Job.getInstance(conf, "Calculate random centroids");
    
    FileInputFormat.setInputPaths(jobCentroids, new Path[] { this.inputPath });
    FileOutputFormat.setOutputPath(jobCentroids, this.outputPath);
    jobCentroids.setJarByClass(getClass());
    
    jobCentroids.setInputFormatClass(SequenceFileInputFormat.class);
    jobCentroids.setOutputFormatClass(TextOutputFormat.class);
    
    jobCentroids.setNumReduceTasks(1);
    
    jobCentroids.setMapperClass(MapCentroids.class);
    jobCentroids.setCombinerClass(CombinerCentroids.class);
    jobCentroids.setReducerClass(ReducerCentroids.class);
    
    jobCentroids.setMapOutputKeyClass(IntWritable.class);
    jobCentroids.setMapOutputValueClass(MapWritable.class);
    
    jobCentroids.setOutputKeyClass(Text.class);
    jobCentroids.setOutputValueClass(Text.class);
    if (indexCentroidMax != -1) {
      jobCentroids.addCacheFile(new URI("CENTROIDS/centroids_" + this.centroid_counter));
    }
    jobCentroids.waitForCompletion(true);
    
    randomCentroidi_unisciFile();
  }
}
