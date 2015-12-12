package org.soa.cenrandom;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerCentroids extends Reducer<IntWritable, Text, Text, Text> {

	private static final Text EMPTY=new Text("");
	private int prob;
	private Counter cs;
	
	public enum REDUCE_OUTPUT {
		COUNTER
	}
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);

		Configuration conf = context.getConfiguration();
		this.cs = context.getCounter(ReducerCentroids.REDUCE_OUTPUT.COUNTER);
		this.prob = conf.getInt("prob", 4) / 8;
		if(this.prob < 1) this.prob = 2;
	}
	

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	
		for (Text doc : values) {
			
			if(this.cs.getValue() != 2) {
				this.cs.increment(1);
				context.write(EMPTY, doc);
			} else
				return;
		}
		
		
	}
	
	
}
