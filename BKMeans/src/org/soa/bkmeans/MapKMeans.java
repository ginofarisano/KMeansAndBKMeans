package org.soa.bkmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public  class MapKMeans extends Mapper<LongWritable, Text, IntWritable, DocumentWritable> {

	private ArrayList<MapWritable> centroids=new ArrayList<MapWritable>();

	private ArrayList<MapWritable> randomCentroids=new ArrayList<MapWritable>();
	
	private int indexCentroid;


	/**
	 *read the centroids file from the hdfs 
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		this.indexCentroid=context.getConfiguration().getInt("index", -1);
		
		URI[] cacheFiles = context.getCacheFiles();

		//random centroidi
		BufferedReader br=new BufferedReader(new FileReader( cacheFiles[0].toString()));

		try {
			String line;
			line=br.readLine();
			MapWritable centroid=new MapWritable();
			while (line != null){
				centroid=parse(line);
				randomCentroids.add(centroid);
				line = br.readLine();
			}
		} finally {
			br.close();
		}

		
		if(indexCentroid != -1){
			//lista tutti i centroidi
			br=new BufferedReader(new FileReader( cacheFiles[1].toString()));
			try {
				String line;
				line=br.readLine();
				MapWritable centroid=new MapWritable();
				while (line != null){
					centroid=parse(line);
					centroids.add(centroid);
					line = br.readLine();
				}
			} finally {
				br.close();
			}
		}


	}
	


	/**
	 * trasforms a line in a MapWritable object
	 * @param line
	 * @return
	 */
	private MapWritable parse(String line) {
		MapWritable centroid=new MapWritable();
		String[] columns = line.split(";");

	
		//the first key is a document name
		for(int i = 1; i < columns.length; i++) {
			//separate key and value
			
			String[] keyAndValue = columns[i].split(":");
			Text keyMap = new Text(keyAndValue[0]);
			DoubleWritable valueMap = new DoubleWritable(Double.parseDouble(keyAndValue[1]));
			centroid.put(keyMap, valueMap);
		}
		return centroid;
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		IntWritable intCentroid = new IntWritable();
		double dist, temp;
		int indexClusterCloser = -1, i;

		DocumentWritable doc = new DocumentWritable();
		doc.setParole(parse(value.toString()));

		//non siamo all'inizio
		if(this.indexCentroid != -1) {
			//controllo l'appartenenza ad index
			dist=Double.POSITIVE_INFINITY;
			i = 0;
			for(MapWritable centroid : this.centroids){
	
				temp = this.calculateDist(centroid, doc.getParole());
				if(temp < dist){
					indexClusterCloser = i;
					dist = temp;
				}
				i++;
			}
		}
	
		if(indexClusterCloser==this.indexCentroid) {
	
			dist=Double.POSITIVE_INFINITY;
			indexClusterCloser = 0;
			i = 0;
			
			for(MapWritable centroid : this.randomCentroids){
	
				temp = this.calculateDist(centroid, doc.getParole());
				if(temp < dist){
					indexClusterCloser = i;
					dist = temp;
				}
				i++;
			}
			
			intCentroid.set(indexClusterCloser);
			context.write(intCentroid, doc);
		}
			
	}

	/**
	 * calculate the distance from centroid and features vector
	 * @param centroid
	 * @param features
	 * @return
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