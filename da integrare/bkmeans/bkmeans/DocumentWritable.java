package org.soa.bkmeans;

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
  private MapWritable parole = new MapWritable();
  private IntWritable docsNum = new IntWritable(0);
  
  public MapWritable getParole()
  {
    return this.parole;
  }
  
  public void setParole(MapWritable parole)
  {
    this.parole = parole;
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
    this.parole.readFields(arg0);
    this.docsNum.readFields(arg0);
  }
  
  public void write(DataOutput arg0)
    throws IOException
  {
    this.parole.write(arg0);
    this.docsNum.write(arg0);
  }
  
  public void merge(DocumentWritable doc)
  {
    Iterator<Map.Entry<Writable, Writable>> iter = doc.getParole().entrySet().iterator();
    
    double value = 0.0D;
    double temp = 0.0D;
    
    DoubleWritable ZERO = new DoubleWritable(0.0D);
    while (iter.hasNext())
    {
      Map.Entry<Writable, Writable> t = (Map.Entry)iter.next();
      value = ((DoubleWritable)t.getValue()).get();
      
      temp = ((DoubleWritable)this.parole.getOrDefault(t.getKey(), ZERO)).get();
      
      value += temp;
      this.parole.put(new Text((Text)t.getKey()), new DoubleWritable(value));
    }
    this.docsNum.set(this.docsNum.get() + doc.getDocsNum().get());
  }
}
