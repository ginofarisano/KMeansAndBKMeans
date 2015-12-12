package org.soa.cenrandom;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.soa.bkmeans.DocumentWritable;

public  class MapCentroids extends Mapper<LongWritable, Text, IntWritable, Text> {

	private ArrayList<MapWritable> centroids=new ArrayList<MapWritable>();
	private int indexSelected;
	private IntWritable indexSelectedWr = new IntWritable();
	private Random rnd = new Random();
	private int prob;
	private Counter cs;
	
	enum MAP_OUTPUT {
		COUNTER
	}

	/**
	 *read the centroids file from the hdfs 
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		//-1 sto al primo giro di kmeans
		this.indexSelected = conf.getInt("index", 0);
		indexSelectedWr.set(indexSelected);
		
		//conto due output almeno
		this.cs = context.getCounter(MapCentroids.MAP_OUTPUT.COUNTER);
		
		
		this.prob = conf.getInt("prob", 4);
		if(this.prob < 1) this.prob = 2;
		
		if(this.indexSelected != -1) {
			URI[] cacheFiles = context.getCacheFiles();
			
			BufferedReader br = new BufferedReader(new FileReader( cacheFiles[0].toString()));
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

	private void manda(Text value, Context context) throws IOException, InterruptedException {
		if(this.cs.getValue() != 2) {
			this.cs.increment(1);
			context.write(indexSelectedWr, value);
			return;
		}
		
		int t = this.rnd.nextInt(this.prob);
		if(this.rnd.nextInt(this.prob) == t)		
			context.write(indexSelectedWr, value);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		
		//non ho centrodi
		if(this.indexSelected == -1) {
			
			manda(value, context);//context.write(indexSelectedWr, value);
			
		} else {
			DocumentWritable doc = new DocumentWritable();
			doc.setParole(parse(value.toString()));
			
			double dist=Double.POSITIVE_INFINITY;
			double temp;
			int indexClusterCloser = 0, i = 0;
			for(MapWritable centroid : this.centroids){
				temp = this.calculateDist(centroid, doc.getParole());
				if(temp < dist){
					indexClusterCloser = i;
					dist = temp;
				}
				i++;
			}
			
			//porto avanti solo il documento che appartiene al cluster di "index"
			if(indexClusterCloser == this.indexSelected) {
				manda(value, context); //context.write(indexSelectedWr, value);
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
			Text keyMap = new Text();
			DoubleWritable valueMap = new DoubleWritable();
			String[] keyAndValue = columns[i].split(":");
			keyMap.set(keyAndValue[0]);
			valueMap.set(Double.parseDouble(keyAndValue[1]));
			centroid.put(keyMap, valueMap);
		}
		return centroid;
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