package org.soa.kmeans;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapKMeans
  extends Mapper<Text, MapWritable, IntWritable, DocumentWritable>
{
  private ArrayList<MapWritable> centroids = new ArrayList();
  int distance;
  
  /* Error */
  protected void setup(Mapper<Text, MapWritable, IntWritable, DocumentWritable>.Context context)
    throws IOException, InterruptedException
  {
    // Byte code:
    //   0: aload_0
    //   1: aload_1
    //   2: invokespecial 33	org/apache/hadoop/mapreduce/Mapper:setup	(Lorg/apache/hadoop/mapreduce/Mapper$Context;)V
    //   5: aload_1
    //   6: invokevirtual 35	org/apache/hadoop/mapreduce/Mapper$Context:getCacheFiles	()[Ljava/net/URI;
    //   9: astore_2
    //   10: new 41	org/apache/hadoop/fs/Path
    //   13: dup
    //   14: aload_2
    //   15: iconst_0
    //   16: aaload
    //   17: invokespecial 43	org/apache/hadoop/fs/Path:<init>	(Ljava/net/URI;)V
    //   20: astore_3
    //   21: aload_1
    //   22: invokevirtual 46	org/apache/hadoop/mapreduce/Mapper$Context:getConfiguration	()Lorg/apache/hadoop/conf/Configuration;
    //   25: invokestatic 50	org/apache/hadoop/fs/FileSystem:get	(Lorg/apache/hadoop/conf/Configuration;)Lorg/apache/hadoop/fs/FileSystem;
    //   28: astore 4
    //   30: aload 4
    //   32: aload_3
    //   33: invokevirtual 56	org/apache/hadoop/fs/FileSystem:open	(Lorg/apache/hadoop/fs/Path;)Lorg/apache/hadoop/fs/FSDataInputStream;
    //   36: astore 5
    //   38: new 60	java/io/BufferedReader
    //   41: dup
    //   42: new 62	java/io/InputStreamReader
    //   45: dup
    //   46: aload 5
    //   48: invokespecial 64	java/io/InputStreamReader:<init>	(Ljava/io/InputStream;)V
    //   51: invokespecial 67	java/io/BufferedReader:<init>	(Ljava/io/Reader;)V
    //   54: astore 6
    //   56: aload 6
    //   58: invokevirtual 70	java/io/BufferedReader:readLine	()Ljava/lang/String;
    //   61: astore 7
    //   63: new 74	org/apache/hadoop/io/MapWritable
    //   66: dup
    //   67: invokespecial 76	org/apache/hadoop/io/MapWritable:<init>	()V
    //   70: astore 8
    //   72: goto +28 -> 100
    //   75: aload_0
    //   76: aload 7
    //   78: invokespecial 77	org/soa/kmeans/MapKMeans:parse	(Ljava/lang/String;)Lorg/apache/hadoop/io/MapWritable;
    //   81: astore 8
    //   83: aload_0
    //   84: getfield 19	org/soa/kmeans/MapKMeans:centroids	Ljava/util/ArrayList;
    //   87: aload 8
    //   89: invokevirtual 81	java/util/ArrayList:add	(Ljava/lang/Object;)Z
    //   92: pop
    //   93: aload 6
    //   95: invokevirtual 70	java/io/BufferedReader:readLine	()Ljava/lang/String;
    //   98: astore 7
    //   100: aload 7
    //   102: ifnonnull -27 -> 75
    //   105: goto +13 -> 118
    //   108: astore 9
    //   110: aload 6
    //   112: invokevirtual 85	java/io/BufferedReader:close	()V
    //   115: aload 9
    //   117: athrow
    //   118: aload 6
    //   120: invokevirtual 85	java/io/BufferedReader:close	()V
    //   123: return
    // Line number table:
    //   Java source line #39	-> byte code offset #0
    //   Java source line #41	-> byte code offset #5
    //   Java source line #43	-> byte code offset #10
    //   Java source line #46	-> byte code offset #21
    //   Java source line #47	-> byte code offset #30
    //   Java source line #48	-> byte code offset #38
    //   Java source line #56	-> byte code offset #56
    //   Java source line #57	-> byte code offset #63
    //   Java source line #58	-> byte code offset #72
    //   Java source line #59	-> byte code offset #75
    //   Java source line #60	-> byte code offset #83
    //   Java source line #61	-> byte code offset #93
    //   Java source line #58	-> byte code offset #100
    //   Java source line #63	-> byte code offset #105
    //   Java source line #64	-> byte code offset #110
    //   Java source line #65	-> byte code offset #115
    //   Java source line #64	-> byte code offset #118
    //   Java source line #66	-> byte code offset #123
    // Local variable table:
    //   start	length	slot	name	signature
    //   0	124	0	this	MapKMeans
    //   0	124	1	context	Mapper<Text, MapWritable, IntWritable, DocumentWritable>.Context
    //   9	6	2	cacheFiles	java.net.URI[]
    //   20	13	3	fileDistrPath	org.apache.hadoop.fs.Path
    //   28	3	4	fs	org.apache.hadoop.fs.FileSystem
    //   36	11	5	sr	org.apache.hadoop.fs.FSDataInputStream
    //   54	65	6	br	java.io.BufferedReader
    //   61	40	7	line	String
    //   70	18	8	centroid	MapWritable
    //   108	8	9	localObject	Object
    // Exception table:
    //   from	to	target	type
    //   56	108	108	finally
  }
  
  private MapWritable parse(String line)
  {
    MapWritable centroid = new MapWritable();
    String[] columns = line.split(";");
    for (int i = 1; i < columns.length; i++)
    {
      String[] keyAndValue = columns[i].split(":");
      Text keyMap = new Text(keyAndValue[0]);
      DoubleWritable valueMap = new DoubleWritable(Double.parseDouble(keyAndValue[1]));
      centroid.put(keyMap, valueMap);
    }
    return centroid;
  }
  
  public void map(Text key, MapWritable value, Mapper<Text, MapWritable, IntWritable, DocumentWritable>.Context context)
    throws IOException, InterruptedException
  {
    IntWritable intCentroid = new IntWritable();
    
    DocumentWritable doc = new DocumentWritable();
    doc.setWords(value);
    
    double dist = Double.POSITIVE_INFINITY;
    
    int indexClusterCloser = 0;
    
    int size = this.centroids.size();
    for (int i = 0; i < size; i++)
    {
      MapWritable centroid = new MapWritable((MapWritable)this.centroids.get(i));
      double temp;
      double temp;
      if (this.distance == 0) {
        temp = calculateDist(centroid, doc.getWords());
      } else {
        temp = calculateDistCosine(centroid, doc.getWords());
      }
      if (temp < dist)
      {
        indexClusterCloser = i;
        dist = temp;
      }
    }
    System.out.println("Selected cluster: " + indexClusterCloser);
    
    intCentroid.set(indexClusterCloser);
    context.write(intCentroid, doc);
  }
  
  public double calculateDist(MapWritable centroid, MapWritable features)
  {
    double dist = 0.0D;
    
    DoubleWritable zero = new DoubleWritable(0.0D);
    
    Iterator<Map.Entry<Writable, Writable>> iter = centroid.entrySet().iterator();
    while (iter.hasNext())
    {
      Map.Entry<Writable, Writable> t = (Map.Entry)iter.next();
      DoubleWritable value = (DoubleWritable)features.getOrDefault((Text)t.getKey(), zero);
      double d = ((DoubleWritable)t.getValue()).get() - value.get();
      dist += d * d;
    }
    iter = features.entrySet().iterator();
    while (iter.hasNext())
    {
      Map.Entry<Writable, Writable> t = (Map.Entry)iter.next();
      if (!centroid.containsKey(t.getKey()))
      {
        double d = ((DoubleWritable)t.getValue()).get();
        dist += d * d;
      }
    }
    return dist;
  }
  
  public double calculateDistCosine(MapWritable centroid, MapWritable features)
  {
    double dividend = 0.0D;double divisor = 0.0D;double aQuad = 0.0D;double bQuad = 0.0D;
    
    DoubleWritable zero = new DoubleWritable(0.0D);
    Iterator<Map.Entry<Writable, Writable>> iter = centroid.entrySet().iterator();
    while (iter.hasNext())
    {
      Map.Entry<Writable, Writable> t = (Map.Entry)iter.next();
      DoubleWritable value = (DoubleWritable)features.getOrDefault((Text)t.getKey(), zero);
      double aVal = ((DoubleWritable)t.getValue()).get();
      dividend += aVal * value.get();
      aQuad += aVal * aVal;
    }
    iter = features.entrySet().iterator();
    while (iter.hasNext())
    {
      Map.Entry<Writable, Writable> t = (Map.Entry)iter.next();
      double bVal = ((DoubleWritable)t.getValue()).get();
      bQuad += bVal * bVal;
    }
    divisor = Math.sqrt(aQuad * bQuad);
    
    return 1.0D - dividend / divisor;
  }
}
