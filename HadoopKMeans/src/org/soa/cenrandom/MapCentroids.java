package org.soa.cenrandom;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

public  class MapCentroids extends Mapper<Text, MapWritable, IntWritable, MapWritable> {

	private IntWritable indexSelectedWr = new IntWritable();
	private Random rnd = new Random();
	private int prob, numbersOfCentroids;
	private Counter cs;
	
	enum MAP_OUTPUT {
		COUNTER
	}

	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
				
		//counter
		this.cs = context.getCounter(MapCentroids.MAP_OUTPUT.COUNTER);
		
		this.numbersOfCentroids = conf.getInt("centroids", 2);
		
		this.prob = conf.getInt("prob", 2);
		if(this.prob < 1) this.prob = 2;
		
	}
	/**
	 * i lead forward numbersOfCentroids centroid + other (with probability prob)
	 */

	@Override
	public void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {

		if(this.cs.getValue() < this.numbersOfCentroids) {
			this.cs.increment(1);
			context.write(indexSelectedWr, value);

		} else {
		
			int t = this.rnd.nextInt(this.prob);
			if(this.rnd.nextInt(this.prob) == t)		
				context.write(indexSelectedWr, value);
		}
	}

}