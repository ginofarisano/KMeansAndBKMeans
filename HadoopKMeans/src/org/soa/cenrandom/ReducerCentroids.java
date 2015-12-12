package org.soa.cenrandom;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerCentroids extends Reducer<IntWritable, MapWritable, Text, MapWritable> {

	private int prob, numbersOfCentroids;
	private Counter cs;
	private Random rnd = new Random();
	
	public enum REDUCE_OUTPUT {
		COUNTER
	}
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		this.cs = context.getCounter(ReducerCentroids.REDUCE_OUTPUT.COUNTER);
		this.numbersOfCentroids = conf.getInt("centroids", 2);
		
		//further lower the probability of cutting
		this.prob = conf.getInt("prob", 2) / 4;
		if(this.prob < 1) this.prob = 2;
	}
	

	@Override
	public void reduce(IntWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

	
		int t, i = 0, sended = 0;
		MapWritable[] bk = new MapWritable[this.numbersOfCentroids];
		
		MapWritable toSend;
		
		for (MapWritable doc : values) {
			
			toSend=new MapWritable(doc);
			
			t = this.rnd.nextInt(this.prob);
			if (this.rnd.nextInt(this.prob) == t && this.cs.getValue() < numbersOfCentroids) {
				context.write(new Text("Centroid"+cs.getValue()), toSend);
				this.cs.increment(1);
			} else {

				if (i < this.numbersOfCentroids) {
					bk[i]=toSend;
					i++;
				}
			}

		}

		for (t = sended,i=0; t < this.numbersOfCentroids; t++,i++) {
			
			if (this.cs.getValue() < numbersOfCentroids) {
				context.write(new Text("Centroid"+cs.getValue()), bk[i]);
				this.cs.increment(1);
			}
			
		}
		
		
	}
	
	
}
