package org.soa.cenrandom;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CombinerCentroids
  extends Reducer<IntWritable, MapWritable, IntWritable, MapWritable>
{
  private Random rnd = new Random();
  private int prob;
  private int numbersOfCentroids;
  
  protected void setup(Reducer<IntWritable, MapWritable, IntWritable, MapWritable>.Context context)
    throws IOException, InterruptedException
  {
    super.setup(context);
    
    Configuration conf = context.getConfiguration();
    this.numbersOfCentroids = conf.getInt("centroids", 2);
    
    this.prob = (conf.getInt("prob", 2) / 2);
    if (this.prob < 1) {
      this.prob = 2;
    }
  }
  
  public void reduce(IntWritable key, Iterable<MapWritable> values, Reducer<IntWritable, MapWritable, IntWritable, MapWritable>.Context context)
    throws IOException, InterruptedException
  {
    int i = 0;int sended = 0;
    
    MapWritable[] bk = new MapWritable[this.numbersOfCentroids];
    for (MapWritable doc : values)
    {
      MapWritable toSend = new MapWritable(doc);
      
      int t = this.rnd.nextInt(this.prob);
      if (this.rnd.nextInt(this.prob) == t)
      {
        sended++;
        context.write(key, toSend);
      }
      else if (i < this.numbersOfCentroids)
      {
        bk[i] = toSend;
        i++;
      }
    }
    int t = sended;
    for (i = 0; t < this.numbersOfCentroids; i++)
    {
      context.write(key, bk[i]);t++;
    }
  }
}
