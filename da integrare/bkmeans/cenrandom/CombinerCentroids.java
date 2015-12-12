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
  
  protected void setup(Reducer<IntWritable, MapWritable, IntWritable, MapWritable>.Context context)
    throws IOException, InterruptedException
  {
    super.setup(context);
    
    Configuration conf = context.getConfiguration();
    this.prob = (conf.getInt("prob", 4) / 4);
    if (this.prob < 1) {
      this.prob = 2;
    }
  }
  
  public void reduce(IntWritable key, Iterable<MapWritable> values, Reducer<IntWritable, MapWritable, IntWritable, MapWritable>.Context context)
    throws IOException, InterruptedException
  {
    int i = 0;int sended = 0;
    MapWritable[] bk = new MapWritable[2];
    for (MapWritable doc : values)
    {
      int t = this.rnd.nextInt(this.prob);
      if (this.rnd.nextInt(this.prob) == t)
      {
        sended++;
        context.write(key, doc);
      }
      else if (i < 2)
      {
        bk[i] = new MapWritable(doc);
        
        i++;
      }
    }
    for (int t = sended; t < 2; t++) {
      context.write(key, bk[t]);
    }
  }
}
