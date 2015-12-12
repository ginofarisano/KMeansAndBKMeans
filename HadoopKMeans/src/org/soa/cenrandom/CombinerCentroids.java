package org.soa.cenrandom;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CombinerCentroids extends
		Reducer<IntWritable, MapWritable, IntWritable, MapWritable> {
	private Random rnd = new Random();
	private int prob, numbersOfCentroids;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		this.numbersOfCentroids = conf.getInt("centroids", 2);
		
		//i lower the probability
		this.prob = conf.getInt("prob", 2) / 2;
		if (this.prob < 1) this.prob = 2;
	}
	/**
	 * i lead forward exactly numbersOfCentroids centroid 
	 * if no centroids are selected there is a stock array that preserve numbersOfCentroids centroids
	 * 
	 */
	@Override
	public void reduce(IntWritable key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {

		int t, i = 0, sended = 0;
		
		MapWritable [] bk = new MapWritable[this.numbersOfCentroids];
		
		MapWritable toSend;
		
		for (MapWritable doc : values) {
			
			toSend=new MapWritable(doc);
			
			t = this.rnd.nextInt(this.prob);
			if (this.rnd.nextInt(this.prob) == t) {
				sended++;
				context.write(key, toSend);
			} else {

				if (i < this.numbersOfCentroids) {
					bk[i]=toSend;
					i++;
				}
			}

		}
		
		// i use the stock array if I did not send exactly numbersOfCentroids centroids
		for (t = sended, i=0; t < this.numbersOfCentroids; t++, i++) {
			
			context.write(key, bk[i]);
		}

	}

}
