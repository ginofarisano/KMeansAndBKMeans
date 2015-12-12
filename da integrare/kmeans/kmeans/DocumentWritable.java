package org.soa.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DocumentWritable
  implements Writable
{
  private MapWritable words = new MapWritable();
  private IntWritable docsNum = new IntWritable(0);
  
  public MapWritable getWords()
  {
    return this.words;
  }
  
  public void setWords(MapWritable words)
  {
    this.words = words;
    this.docsNum.set(1);
  }
  
  public IntWritable getDocsNum()
  {
    return this.docsNum;
  }
  
  public void setDocsNum(IntWritable docsNum)
  {
    this.docsNum = docsNum;
  }
  
  public void readFields(DataInput arg0)
    throws IOException
  {
    this.words.readFields(arg0);
    this.docsNum.readFields(arg0);
  }
  
  public void write(DataOutput arg0)
    throws IOException
  {
    this.words.write(arg0);
    this.docsNum.write(arg0);
  }
  
  public void merge(DocumentWritable doc)
  {
    Iterator<Map.Entry<Writable, Writable>> iter = doc.getWords().entrySet().iterator();
    
    double value = 0.0D;
    double temp = 0.0D;
    
    DoubleWritable ZERO = new DoubleWritable(0.0D);
    while (iter.hasNext())
    {
      Map.Entry<Writable, Writable> t = (Map.Entry)iter.next();
      value = ((DoubleWritable)t.getValue()).get();
      
      temp = ((DoubleWritable)this.words.getOrDefault(t.getKey(), ZERO)).get();
      
      value += temp;
      this.words.put((Writable)t.getKey(), new DoubleWritable(value));
    }
    this.docsNum.set(this.docsNum.get() + doc.getDocsNum().get());
  }
  
  public Text printResult()
  {
    StringBuilder result = new StringBuilder();
    
    result.append("\nThe cluster is composed of " + this.docsNum.get() + " document\nCentroid:\n");
    
    Iterator<Map.Entry<Writable, Writable>> iter = this.words.entrySet().iterator();
    while (iter.hasNext())
    {
      Map.Entry<Writable, Writable> t = (Map.Entry)iter.next();
      
      double value = ((DoubleWritable)t.getValue()).get();
      
      String key = ((Text)t.getKey()).toString();
      value = ((DoubleWritable)t.getValue()).get();
      
      result.append(key + ":" + value + "   ");
    }
    return new Text(result.toString());
  }
}
