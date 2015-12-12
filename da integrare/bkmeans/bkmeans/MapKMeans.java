package org.soa.bkmeans;

import java.io.IOException;
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
  private ArrayList<MapWritable> randomCentroids = new ArrayList();
  private int indexCentroid;
  
  /* Error */
  protected void setup(Mapper<Text, MapWritable, IntWritable, DocumentWritable>.Context context)
    throws IOException, InterruptedException
  {
    // Byte code:
    //   0: aload_0
    //   1: aload_1
    //   2: invokespecial 36	org/apache/hadoop/mapreduce/Mapper:setup	(Lorg/apache/hadoop/mapreduce/Mapper$Context;)V
    //   5: aload_0
    //   6: aload_1
    //   7: invokevirtual 38	org/apache/hadoop/mapreduce/Mapper$Context:getConfiguration	()Lorg/apache/hadoop/conf/Configuration;
    //   10: ldc 44
    //   12: iconst_m1
    //   13: invokevirtual 46	org/apache/hadoop/conf/Configuration:getInt	(Ljava/lang/String;I)I
    //   16: putfield 52	org/soa/bkmeans/MapKMeans:indexCentroid	I
    //   19: aload_1
    //   20: invokevirtual 54	org/apache/hadoop/mapreduce/Mapper$Context:getCacheFiles	()[Ljava/net/URI;
    //   23: astore_2
    //   24: new 58	org/apache/hadoop/fs/Path
    //   27: dup
    //   28: aload_2
    //   29: iconst_0
    //   30: aaload
    //   31: invokespecial 60	org/apache/hadoop/fs/Path:<init>	(Ljava/net/URI;)V
    //   34: astore_3
    //   35: aload_1
    //   36: invokevirtual 38	org/apache/hadoop/mapreduce/Mapper$Context:getConfiguration	()Lorg/apache/hadoop/conf/Configuration;
    //   39: invokestatic 63	org/apache/hadoop/fs/FileSystem:get	(Lorg/apache/hadoop/conf/Configuration;)Lorg/apache/hadoop/fs/FileSystem;
    //   42: astore 4
    //   44: aload 4
    //   46: aload_3
    //   47: invokevirtual 69	org/apache/hadoop/fs/FileSystem:open	(Lorg/apache/hadoop/fs/Path;)Lorg/apache/hadoop/fs/FSDataInputStream;
    //   50: astore 5
    //   52: new 73	java/io/BufferedReader
    //   55: dup
    //   56: new 75	java/io/InputStreamReader
    //   59: dup
    //   60: aload 5
    //   62: invokespecial 77	java/io/InputStreamReader:<init>	(Ljava/io/InputStream;)V
    //   65: invokespecial 80	java/io/BufferedReader:<init>	(Ljava/io/Reader;)V
    //   68: astore 6
    //   70: aload 6
    //   72: invokevirtual 83	java/io/BufferedReader:readLine	()Ljava/lang/String;
    //   75: astore 7
    //   77: new 87	org/apache/hadoop/io/MapWritable
    //   80: dup
    //   81: invokespecial 89	org/apache/hadoop/io/MapWritable:<init>	()V
    //   84: astore 8
    //   86: goto +28 -> 114
    //   89: aload_0
    //   90: aload 7
    //   92: invokespecial 90	org/soa/bkmeans/MapKMeans:parse	(Ljava/lang/String;)Lorg/apache/hadoop/io/MapWritable;
    //   95: astore 8
    //   97: aload_0
    //   98: getfield 22	org/soa/bkmeans/MapKMeans:randomCentroids	Ljava/util/ArrayList;
    //   101: aload 8
    //   103: invokevirtual 94	java/util/ArrayList:add	(Ljava/lang/Object;)Z
    //   106: pop
    //   107: aload 6
    //   109: invokevirtual 83	java/io/BufferedReader:readLine	()Ljava/lang/String;
    //   112: astore 7
    //   114: aload 7
    //   116: ifnonnull -27 -> 89
    //   119: goto +13 -> 132
    //   122: astore 9
    //   124: aload 6
    //   126: invokevirtual 98	java/io/BufferedReader:close	()V
    //   129: aload 9
    //   131: athrow
    //   132: aload 6
    //   134: invokevirtual 98	java/io/BufferedReader:close	()V
    //   137: aload_0
    //   138: getfield 52	org/soa/bkmeans/MapKMeans:indexCentroid	I
    //   141: iconst_m1
    //   142: if_icmpeq +107 -> 249
    //   145: new 58	org/apache/hadoop/fs/Path
    //   148: dup
    //   149: aload_2
    //   150: iconst_1
    //   151: aaload
    //   152: invokespecial 60	org/apache/hadoop/fs/Path:<init>	(Ljava/net/URI;)V
    //   155: astore_3
    //   156: aload 4
    //   158: aload_3
    //   159: invokevirtual 69	org/apache/hadoop/fs/FileSystem:open	(Lorg/apache/hadoop/fs/Path;)Lorg/apache/hadoop/fs/FSDataInputStream;
    //   162: astore 5
    //   164: new 73	java/io/BufferedReader
    //   167: dup
    //   168: new 75	java/io/InputStreamReader
    //   171: dup
    //   172: aload 5
    //   174: invokespecial 77	java/io/InputStreamReader:<init>	(Ljava/io/InputStream;)V
    //   177: invokespecial 80	java/io/BufferedReader:<init>	(Ljava/io/Reader;)V
    //   180: astore 6
    //   182: aload 6
    //   184: invokevirtual 83	java/io/BufferedReader:readLine	()Ljava/lang/String;
    //   187: astore 7
    //   189: new 87	org/apache/hadoop/io/MapWritable
    //   192: dup
    //   193: invokespecial 89	org/apache/hadoop/io/MapWritable:<init>	()V
    //   196: astore 8
    //   198: goto +28 -> 226
    //   201: aload_0
    //   202: aload 7
    //   204: invokespecial 90	org/soa/bkmeans/MapKMeans:parse	(Ljava/lang/String;)Lorg/apache/hadoop/io/MapWritable;
    //   207: astore 8
    //   209: aload_0
    //   210: getfield 20	org/soa/bkmeans/MapKMeans:centroids	Ljava/util/ArrayList;
    //   213: aload 8
    //   215: invokevirtual 94	java/util/ArrayList:add	(Ljava/lang/Object;)Z
    //   218: pop
    //   219: aload 6
    //   221: invokevirtual 83	java/io/BufferedReader:readLine	()Ljava/lang/String;
    //   224: astore 7
    //   226: aload 7
    //   228: ifnonnull -27 -> 201
    //   231: goto +13 -> 244
    //   234: astore 9
    //   236: aload 6
    //   238: invokevirtual 98	java/io/BufferedReader:close	()V
    //   241: aload 9
    //   243: athrow
    //   244: aload 6
    //   246: invokevirtual 98	java/io/BufferedReader:close	()V
    //   249: return
    // Line number table:
    //   Java source line #39	-> byte code offset #0
    //   Java source line #41	-> byte code offset #5
    //   Java source line #43	-> byte code offset #19
    //   Java source line #45	-> byte code offset #24
    //   Java source line #48	-> byte code offset #35
    //   Java source line #49	-> byte code offset #44
    //   Java source line #50	-> byte code offset #52
    //   Java source line #58	-> byte code offset #70
    //   Java source line #59	-> byte code offset #77
    //   Java source line #60	-> byte code offset #86
    //   Java source line #61	-> byte code offset #89
    //   Java source line #62	-> byte code offset #97
    //   Java source line #63	-> byte code offset #107
    //   Java source line #60	-> byte code offset #114
    //   Java source line #65	-> byte code offset #119
    //   Java source line #66	-> byte code offset #124
    //   Java source line #67	-> byte code offset #129
    //   Java source line #66	-> byte code offset #132
    //   Java source line #70	-> byte code offset #137
    //   Java source line #73	-> byte code offset #145
    //   Java source line #77	-> byte code offset #156
    //   Java source line #78	-> byte code offset #164
    //   Java source line #91	-> byte code offset #182
    //   Java source line #92	-> byte code offset #189
    //   Java source line #93	-> byte code offset #198
    //   Java source line #94	-> byte code offset #201
    //   Java source line #95	-> byte code offset #209
    //   Java source line #96	-> byte code offset #219
    //   Java source line #93	-> byte code offset #226
    //   Java source line #98	-> byte code offset #231
    //   Java source line #99	-> byte code offset #236
    //   Java source line #100	-> byte code offset #241
    //   Java source line #99	-> byte code offset #244
    //   Java source line #104	-> byte code offset #249
    // Local variable table:
    //   start	length	slot	name	signature
    //   0	250	0	this	MapKMeans
    //   0	250	1	context	Mapper<Text, MapWritable, IntWritable, DocumentWritable>.Context
    //   23	127	2	cacheFiles	java.net.URI[]
    //   34	125	3	fileDistrPath	org.apache.hadoop.fs.Path
    //   42	115	4	fs	org.apache.hadoop.fs.FileSystem
    //   50	123	5	sr	org.apache.hadoop.fs.FSDataInputStream
    //   68	177	6	br	java.io.BufferedReader
    //   75	40	7	line	String
    //   187	40	7	line	String
    //   84	18	8	centroid	MapWritable
    //   196	18	8	centroid	MapWritable
    //   122	8	9	localObject1	Object
    //   234	8	9	localObject2	Object
    // Exception table:
    //   from	to	target	type
    //   70	122	122	finally
    //   182	234	234	finally
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
    
    int indexClusterCloser = -1;
    
    DocumentWritable doc = new DocumentWritable();
    doc.setParole(value);
    if (this.indexCentroid != -1)
    {
      double dist = Double.POSITIVE_INFINITY;
      int i = 0;
      for (MapWritable centroid : this.centroids)
      {
        double temp = calculateDist(centroid, doc.getParole());
        if (temp < dist)
        {
          indexClusterCloser = i;
          dist = temp;
        }
        i++;
      }
    }
    if (indexClusterCloser == this.indexCentroid)
    {
      double dist = Double.POSITIVE_INFINITY;
      indexClusterCloser = 0;
      int i = 0;
      for (MapWritable centroid : this.randomCentroids)
      {
        double temp = calculateDistCosine(centroid, doc.getParole());
        if (temp < dist)
        {
          indexClusterCloser = i;
          dist = temp;
        }
        i++;
      }
      intCentroid.set(indexClusterCloser);
      context.write(intCentroid, doc);
    }
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
