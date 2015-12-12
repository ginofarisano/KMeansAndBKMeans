package org.soa.cenrandom;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerCentroids
  extends Reducer<IntWritable, MapWritable, Text, Text>
{
  private static final Text EMPTY = new Text("");
  private int prob;
  private Counter cs;
  
  public static enum REDUCE_OUTPUT
  {
    COUNTER;
  }
  
  protected void setup(Reducer<IntWritable, MapWritable, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
    super.setup(context);
    
    Configuration conf = context.getConfiguration();
    this.cs = context.getCounter(REDUCE_OUTPUT.COUNTER);
    this.prob = (conf.getInt("prob", 4) / 8);
    if (this.prob < 1) {
      this.prob = 2;
    }
  }
  
  public void reduce(IntWritable key, Iterable<MapWritable> values, Reducer<IntWritable, MapWritable, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
    for (MapWritable doc : values) {
      if (this.cs.getValue() != 2L)
      {
        this.cs.increment(1L);
        
        context.write(EMPTY, print(0, doc));
      }
      else
      {
        return;
      }
    }
  }
  
  public Text print(int n, MapWritable finalMap)
  {
    String outString = n + ";";
    
    Iterator<Map.Entry<Writable, Writable>> iter = finalMap.entrySet().iterator();
    while (iter.hasNext())
    {
      Map.Entry<Writable, Writable> t = (Map.Entry)iter.next();
      outString = outString + ((Text)t.getKey()).toString() + ":" + ((DoubleWritable)t.getValue()).get() + ";";
    }
    return new Text(outString);
  }
}
