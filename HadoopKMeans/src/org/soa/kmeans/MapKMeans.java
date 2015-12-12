package org.soa.kmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public  class MapKMeans extends Mapper<Text, MapWritable, IntWritable, DocumentWritable> {

	private ArrayList<MapWritable> centroids=new ArrayList<MapWritable>();


	/**
	 *read the centroids file from the hdfs 
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		URI[] cacheFiles = context.getCacheFiles();
		//new Path(cacheFiles[0])

		Path seqFilePath = new Path(cacheFiles[0]);

		Configuration conf = new Configuration();

		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(seqFilePath));

		Text key = new Text();
		MapWritable val = new MapWritable();

		MapWritable map=null;

		while (reader.next(key, val)) {
			map=new MapWritable(val);
			centroids.add(map);
		}
		
		reader.close();
	}

	@Override
	public void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {

		IntWritable intCentroid = new IntWritable();

		DocumentWritable doc = new DocumentWritable();
		doc.setWords(value);

		double dist=Double.POSITIVE_INFINITY;
		double temp;
		int indexClusterCloser = 0;
		MapWritable centroid;


		int size=centroids.size();
		for(int i=0; i<size;i++){
			centroid=new MapWritable(centroids.get(i));
			temp = this.calculateDist(centroid, doc.getWords());
			if(temp <= dist){
				indexClusterCloser = i;
				dist = temp;
			}

		}

		System.out.println("Selected cluster: "+indexClusterCloser);

		intCentroid.set(indexClusterCloser);
		context.write(intCentroid, doc);
	}

	/**
	 * calculate the distance from centroid and features vector
	 */
	public  double calculateDist(MapWritable centroid, MapWritable features) {
		double dist = 0;
		double d;
		//iterator of the map
		DoubleWritable zero = new DoubleWritable(0.0);
		//iter first the centroids
		Iterator<Entry<Writable,Writable>> iter = centroid.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Writable, Writable> t = iter.next();
			DoubleWritable value = (DoubleWritable) features.getOrDefault((Text)t.getKey(), zero);  //getOrDefault
			d = ((DoubleWritable)t.getValue()).get() - value.get();
			dist += d*d;
		}
		//value that are in features and there are not in centroid
		iter = features.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Writable, Writable> t = iter.next();
			if(!centroid.containsKey(t.getKey())) {
				d = ((DoubleWritable)t.getValue()).get();
				dist += d*d;
			}
		}
		return dist;
	}
}