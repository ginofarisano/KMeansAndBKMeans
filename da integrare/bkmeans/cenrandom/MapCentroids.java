package org.soa.cenrandom;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapCentroids
  extends Mapper<Text, MapWritable, IntWritable, MapWritable>
{
  private ArrayList<MapWritable> centroids = new ArrayList();
  private int indexSelected;
  private IntWritable indexSelectedWr = new IntWritable();
  private Random rnd = new Random();
  private int prob;
  private Counter cs;
  /* Error */
  protected void setup(Mapper<Text, MapWritable, IntWritable, MapWritable>.Context context)
    throws IOException, InterruptedException
  {
    // Byte code:
    //   0: aload_0
    //   1: aload_1
    //   2: invokespecial 50	org/apache/hadoop/mapreduce/Mapper:setup	(Lorg/apache/hadoop/mapreduce/Mapper$Context;)V
    //   5: aload_1
    //   6: invokevirtual 52	org/apache/hadoop/mapreduce/Mapper$Context:getConfiguration	()Lorg/apache/hadoop/conf/Configuration;
    //   9: astore_2
    //   10: aload_0
    //   11: aload_2
    //   12: ldc 58
    //   14: iconst_0
    //   15: invokevirtual 60	org/apache/hadoop/conf/Configuration:getInt	(Ljava/lang/String;I)I
    //   18: putfield 66	org/soa/cenrandom/MapCentroids:indexSelected	I
    //   21: aload_0
    //   22: getfield 31	org/soa/cenrandom/MapCentroids:indexSelectedWr	Lorg/apache/hadoop/io/IntWritable;
    //   25: aload_0
    //   26: getfield 66	org/soa/cenrandom/MapCentroids:indexSelected	I
    //   29: invokevirtual 68	org/apache/hadoop/io/IntWritable:set	(I)V
    //   32: aload_0
    //   33: aload_1
    //   34: getstatic 72	org/soa/cenrandom/MapCentroids$MAP_OUTPUT:COUNTER	Lorg/soa/cenrandom/MapCentroids$MAP_OUTPUT;
    //   37: invokevirtual 78	org/apache/hadoop/mapreduce/Mapper$Context:getCounter	(Ljava/lang/Enum;)Lorg/apache/hadoop/mapreduce/Counter;
    //   40: putfield 82	org/soa/cenrandom/MapCentroids:cs	Lorg/apache/hadoop/mapreduce/Counter;
    //   43: aload_0
    //   44: aload_2
    //   45: ldc 84
    //   47: iconst_4
    //   48: invokevirtual 60	org/apache/hadoop/conf/Configuration:getInt	(Ljava/lang/String;I)I
    //   51: putfield 85	org/soa/cenrandom/MapCentroids:prob	I
    //   54: aload_0
    //   55: getfield 85	org/soa/cenrandom/MapCentroids:prob	I
    //   58: iconst_1
    //   59: if_icmpge +8 -> 67
    //   62: aload_0
    //   63: iconst_2
    //   64: putfield 85	org/soa/cenrandom/MapCentroids:prob	I
    //   67: aload_0
    //   68: getfield 66	org/soa/cenrandom/MapCentroids:indexSelected	I
    //   71: iconst_m1
    //   72: if_icmpeq +123 -> 195
    //   75: aload_1
    //   76: invokevirtual 87	org/apache/hadoop/mapreduce/Mapper$Context:getCacheFiles	()[Ljava/net/URI;
    //   79: astore_3
    //   80: new 91	org/apache/hadoop/fs/Path
    //   83: dup
    //   84: aload_3
    //   85: iconst_0
    //   86: aaload
    //   87: invokespecial 93	org/apache/hadoop/fs/Path:<init>	(Ljava/net/URI;)V
    //   90: astore 4
    //   92: aload_1
    //   93: invokevirtual 52	org/apache/hadoop/mapreduce/Mapper$Context:getConfiguration	()Lorg/apache/hadoop/conf/Configuration;
    //   96: invokestatic 96	org/apache/hadoop/fs/FileSystem:get	(Lorg/apache/hadoop/conf/Configuration;)Lorg/apache/hadoop/fs/FileSystem;
    //   99: astore 5
    //   101: aload 5
    //   103: aload 4
    //   105: invokevirtual 102	org/apache/hadoop/fs/FileSystem:open	(Lorg/apache/hadoop/fs/Path;)Lorg/apache/hadoop/fs/FSDataInputStream;
    //   108: astore 6
    //   110: new 106	java/io/BufferedReader
    //   113: dup
    //   114: new 108	java/io/InputStreamReader
    //   117: dup
    //   118: aload 6
    //   120: invokespecial 110	java/io/InputStreamReader:<init>	(Ljava/io/InputStream;)V
    //   123: invokespecial 113	java/io/BufferedReader:<init>	(Ljava/io/Reader;)V
    //   126: astore 7
    //   128: aload 7
    //   130: invokevirtual 116	java/io/BufferedReader:readLine	()Ljava/lang/String;
    //   133: astore 8
    //   135: new 120	org/apache/hadoop/io/MapWritable
    //   138: dup
    //   139: invokespecial 122	org/apache/hadoop/io/MapWritable:<init>	()V
    //   142: astore 9
    //   144: goto +28 -> 172
    //   147: aload_0
    //   148: aload 8
    //   150: invokespecial 123	org/soa/cenrandom/MapCentroids:parse	(Ljava/lang/String;)Lorg/apache/hadoop/io/MapWritable;
    //   153: astore 9
    //   155: aload_0
    //   156: getfield 26	org/soa/cenrandom/MapCentroids:centroids	Ljava/util/ArrayList;
    //   159: aload 9
    //   161: invokevirtual 127	java/util/ArrayList:add	(Ljava/lang/Object;)Z
    //   164: pop
    //   165: aload 7
    //   167: invokevirtual 116	java/io/BufferedReader:readLine	()Ljava/lang/String;
    //   170: astore 8
    //   172: aload 8
    //   174: ifnonnull -27 -> 147
    //   177: goto +13 -> 190
    //   180: astore 10
    //   182: aload 7
    //   184: invokevirtual 131	java/io/BufferedReader:close	()V
    //   187: aload 10
    //   189: athrow
    //   190: aload 7
    //   192: invokevirtual 131	java/io/BufferedReader:close	()V
    //   195: return
    // Line number table:
    //   Java source line #46	-> byte code offset #0
    //   Java source line #48	-> byte code offset #5
    //   Java source line #50	-> byte code offset #10
    //   Java source line #51	-> byte code offset #21
    //   Java source line #54	-> byte code offset #32
    //   Java source line #57	-> byte code offset #43
    //   Java source line #58	-> byte code offset #54
    //   Java source line #62	-> byte code offset #67
    //   Java source line #63	-> byte code offset #75
    //   Java source line #65	-> byte code offset #80
    //   Java source line #67	-> byte code offset #92
    //   Java source line #68	-> byte code offset #101
    //   Java source line #69	-> byte code offset #110
    //   Java source line #73	-> byte code offset #128
    //   Java source line #74	-> byte code offset #135
    //   Java source line #75	-> byte code offset #144
    //   Java source line #76	-> byte code offset #147
    //   Java source line #77	-> byte code offset #155
    //   Java source line #78	-> byte code offset #165
    //   Java source line #75	-> byte code offset #172
    //   Java source line #80	-> byte code offset #177
    //   Java source line #81	-> byte code offset #182
    //   Java source line #82	-> byte code offset #187
    //   Java source line #81	-> byte code offset #190
    //   Java source line #85	-> byte code offset #195
    // Local variable table:
    //   start	length	slot	name	signature
    //   0	196	0	this	MapCentroids
    //   0	196	1	context	Mapper<Text, MapWritable, IntWritable, MapWritable>.Context
    //   9	36	2	conf	org.apache.hadoop.conf.Configuration
    //   79	6	3	cacheFiles	java.net.URI[]
    //   90	14	4	fileDistrPath	org.apache.hadoop.fs.Path
    //   99	3	5	fs	org.apache.hadoop.fs.FileSystem
    //   108	11	6	sr	org.apache.hadoop.fs.FSDataInputStream
    //   126	65	7	br	java.io.BufferedReader
    //   133	40	8	line	String
    //   142	18	9	centroid	MapWritable
    //   180	8	10	localObject	Object
    // Exception table:
    //   from	to	target	type
    //   128	180	180	finally
  }
  
  static enum MAP_OUTPUT
  {
    COUNTER;
  }
  
  private void manda(MapWritable value, Mapper<Text, MapWritable, IntWritable, MapWritable>.Context context)
    throws IOException, InterruptedException
  {
    if (this.cs.getValue() != 2L)
    {
      this.cs.increment(1L);
      context.write(this.indexSelectedWr, value);
      return;
    }
    int t = this.rnd.nextInt(this.prob);
    if (this.rnd.nextInt(this.prob) == t) {
      context.write(this.indexSelectedWr, value);
    }
  }
  
  public void map(Text key, MapWritable value, Mapper<Text, MapWritable, IntWritable, MapWritable>.Context context)
    throws IOException, InterruptedException
  {
    if (this.indexSelected == -1)
    {
      manda(value, context);
    }
    else
    {
      double dist = Double.POSITIVE_INFINITY;
      
      int indexClusterCloser = 0;int i = 0;
      for (MapWritable centroid : this.centroids)
      {
        double temp = calculateDist(centroid, value);
        if (temp < dist)
        {
          indexClusterCloser = i;
          dist = temp;
        }
        i++;
      }
      if (indexClusterCloser == this.indexSelected) {
        manda(value, context);
      }
    }
  }
  
  private MapWritable parse(String line)
  {
    MapWritable centroid = new MapWritable();
    String[] columns = line.split(";");
    for (int i = 1; i < columns.length; i++)
    {
      Text keyMap = new Text();
      DoubleWritable valueMap = new DoubleWritable();
      String[] keyAndValue = columns[i].split(":");
      keyMap.set(keyAndValue[0]);
      valueMap.set(Double.parseDouble(keyAndValue[1]));
      centroid.put(keyMap, valueMap);
    }
    return centroid;
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
}
