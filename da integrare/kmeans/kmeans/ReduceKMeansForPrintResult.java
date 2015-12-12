package org.soa.kmeans;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReduceKMeansForPrintResult
  extends Reducer<IntWritable, DocumentWritable, Text, Text>
{
  public void everage(DocumentWritable centroide)
  {
    double numOfDocs = centroide.getDocsNum().get();
    MapWritable finalMap = centroide.getWords();
    
    Iterator<Map.Entry<Writable, Writable>> iter = finalMap.entrySet().iterator();
    while (iter.hasNext())
    {
      Map.Entry<Writable, Writable> t = (Map.Entry)iter.next();
      
      double value = ((DoubleWritable)t.getValue()).get() / numOfDocs;
      finalMap.put((Writable)t.getKey(), new DoubleWritable(value));
    }
  }
  
  public void reduce(IntWritable key, Iterable<DocumentWritable> values, Reducer<IntWritable, DocumentWritable, Text, Text>.Context context)
    throws IOException, InterruptedException
  {
    DocumentWritable newCentroid = new DocumentWritable();
    for (DocumentWritable pseudoCentroid : values) {
      newCentroid.merge(pseudoCentroid);
    }
    everage(newCentroid);
    
    context.write(new Text("Centroid" + key.get()), newCentroid.printResult());
  }
}
