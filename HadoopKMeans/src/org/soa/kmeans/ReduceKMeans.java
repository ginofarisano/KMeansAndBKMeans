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
 * calculate the mean of the centroids
 * remember that the combine has the partial number of document
 * 
 */
public  class ReduceKMeans extends Reducer<IntWritable, DocumentWritable, Text, MapWritable> {
	
	/**
	 * divides the key of the words for the number of documents
	 * @param finalMap
	 */
	public void everage(DocumentWritable centroide) {
		double numOfDocs = centroide.getDocsNum().get();
		MapWritable finalMap = centroide.getWords();

		double value;
		Iterator<Entry<Writable, Writable>> iter = finalMap.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Writable, Writable> t = iter.next();
			
			value = ((DoubleWritable)t.getValue()).get() / numOfDocs;
			finalMap.put(t.getKey(), new DoubleWritable(value));
		}
	}
	

	@Override
	public void reduce(IntWritable key, Iterable<DocumentWritable> values, Context context) throws IOException, InterruptedException {

		DocumentWritable newCentroid = new DocumentWritable();

		for (DocumentWritable pseudoCentroid : values) {
			newCentroid.merge(pseudoCentroid);
		}
		

		everage(newCentroid);
		
		context.write(new Text("Centroid"+key.get()),newCentroid.getWords());
	}
}
