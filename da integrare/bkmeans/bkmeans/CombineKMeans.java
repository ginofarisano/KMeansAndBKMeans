package org.soa.bkmeans;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CombineKMeans
  extends Reducer<IntWritable, DocumentWritable, IntWritable, DocumentWritable>
{
  private static final DoubleWritable ZERO = new DoubleWritable(0.0D);
  
  public void reduce(IntWritable key, Iterable<DocumentWritable> values, Reducer<IntWritable, DocumentWritable, IntWritable, DocumentWritable>.Context context)
    throws IOException, InterruptedException
  {
    DocumentWritable pseudoCentroid = new DocumentWritable();
    for (DocumentWritable doc : values) {
      pseudoCentroid.merge(doc);
    }
    context.write(key, pseudoCentroid);
  }
}
