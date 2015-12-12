package org.soa.bkmeans;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReduceKMeans
  extends Reducer<IntWritable, DocumentWritable, Text, Text>
{
  private static final Text EMPTY = new Text("");
  private long maxCentroidWords = 0L;
  
  protected void setup(Reducer<IntWritable, DocumentWritable, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
    super.setup(context);
    
    this.maxCentroidWords = context.getConfiguration().getLong("maxCentroidWords", -1L);
  }
  
  public MapWritable everage(DocumentWritable centroide)
  {
    double numOfDocs = centroide.getDocsNum().get();
    
    MapWritable finalMap = centroide.getParole();
    
    SortedMapWritable sortedMap = new SortedMapWritable();
    
    Iterator<Map.Entry<Writable, Writable>> iter = finalMap.entrySet().iterator();
    while (iter.hasNext())
    {
      Map.Entry<Writable, Writable> t = (Map.Entry)iter.next();
      
      double value = ((DoubleWritable)t.getValue()).get() / numOfDocs;
      if (sortedMap.size() == this.maxCentroidWords)
      {
        WritableComparable a = sortedMap.firstKey();
        
        double valueToRemove = ((DoubleWritable)a).get();
        if (valueToRemove < value)
        {
          sortedMap.remove(a);
          sortedMap.put(new DoubleWritable(value), (Writable)t.getKey());
        }
      }
      else
      {
        sortedMap.put(new DoubleWritable(value), (Writable)t.getKey());
      }
    }
    Iterator<Map.Entry<WritableComparable, Writable>> iteratorSortedMap = sortedMap.entrySet().iterator();
    
    MapWritable toReturn = new MapWritable();
    while (iteratorSortedMap.hasNext())
    {
      Map.Entry<WritableComparable, Writable> entry = (Map.Entry)iteratorSortedMap.next();
      
      toReturn.put((Writable)entry.getValue(), (Writable)entry.getKey());
    }
    return toReturn;
  }
  
  public Text print(int n, MapWritable finalMap)
  {
    String outString = n + ";";
    
    Iterator<Map.Entry<Writable, Writable>> iter = finalMap.entrySet().iterator();
    
    long counter = 0L;
    while ((iter.hasNext()) && (counter < this.maxCentroidWords))
    {
      Map.Entry<Writable, Writable> t = (Map.Entry)iter.next();
      outString = outString + ((Text)t.getKey()).toString() + ":" + ((DoubleWritable)t.getValue()).get() + ";";
      
      counter += 1L;
    }
    return new Text(outString);
  }
  
  public void reduce(IntWritable key, Iterable<DocumentWritable> values, Reducer<IntWritable, DocumentWritable, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
    DocumentWritable newCentroid = new DocumentWritable();
    for (DocumentWritable pseudoCentroid : values) {
      newCentroid.merge(pseudoCentroid);
    }
    MapWritable optimizedCentroid = everage(newCentroid);
    Text toReturn = print(newCentroid.getDocsNum().get(), optimizedCentroid);
    context.write(EMPTY, toReturn);
  }
}
