package org.soa.kmeans;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * merge all the document in the same cluster
 * sum the values of the words and maintains the count of the
 * document in the same cluster
 * 
 * @author user
 *
 */
public  class CombineKMeans extends Reducer<IntWritable, DocumentWritable, IntWritable, DocumentWritable> {
	
	private static final DoubleWritable ZERO=new DoubleWritable(0);
	
	@Override
	public void reduce(IntWritable key, Iterable<DocumentWritable> values, Context context) throws IOException, InterruptedException {
		
		DocumentWritable pseudoCentroid = new DocumentWritable();
		
		for (DocumentWritable doc : values) {
			pseudoCentroid.merge(doc);
		}
		
		context.write(key, pseudoCentroid);
	}
	

	/*public void merge(MapWritable intermediate,MapWritable toMerge){
		
		Iterator<Entry<Writable, Writable>> iter = toMerge.entrySet().iterator();

		double value=0;
		double temp=0;

		while(iter.hasNext()) {
			Entry<Writable, Writable> t = iter.next();
			value = ((DoubleWritable)t.getValue()).get();
			//the intermediate vector has the word in the toMerge vector?
			temp = ((DoubleWritable) intermediate.getOrDefault(t.getKey(), ZERO)).get(); 
			//modify or add the value
			value += temp;
			intermediate.put(t.getKey(),new DoubleWritable(value));
		}
			//modify the partial numbers of the document in the cluster
			temp=((DoubleWritable)intermediate.get(NOUMBERSOFDOCUMENT)).get();
			temp=temp+1;
			intermediate.put(NOUMBERSOFDOCUMENT, new DoubleWritable(temp));
	}*/
}