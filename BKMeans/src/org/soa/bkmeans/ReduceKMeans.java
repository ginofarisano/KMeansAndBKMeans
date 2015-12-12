package org.soa.bkmeans;

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
 * @author user
 *
 */
public  class ReduceKMeans extends Reducer<IntWritable, DocumentWritable, Text, Text> {

	private static final Text EMPTY=new Text("");


	/*public void merge(MapWritable finalMap,MapWritable toMerge){

		Iterator<Entry<Writable, Writable>> iter = toMerge.entrySet().iterator();
		double value=0,temp=0;
		DoubleWritable zero=new DoubleWritable(0.0);
		String what;

		while(iter.hasNext()) {
			Entry<Writable, Writable> t = iter.next();

			what=((Text)t.getKey()).toString();
			//if the key is the count of document continue. see the combine's fase
			if(what.equals(NOUMBERSOFDOCUMENT))
				continue;

			temp = ((DoubleWritable)t.getValue()).get();
			//the final map contains the key?
			value = ((DoubleWritable) finalMap.getOrDefault(t.getKey(), ZERO)).get(); 

			value += temp;
			//update the value of a key
			finalMap.put(t.getKey(),new DoubleWritable(value));

		}
		//update the numbers of document
		double temp1=((DoubleWritable)finalMap.get(NOUMBERSOFDOCUMENT)).get();
		double temp2=((DoubleWritable)toMerge.get(NOUMBERSOFDOCUMENT)).get();
		finalMap.put(NOUMBERSOFDOCUMENT, new DoubleWritable(temp1+temp2));

	}*/
	
	/**
	 * divides the key of the words for the number of documents
	 * @param finalMap
	 */
	public void everage(DocumentWritable centroide) {
		double numOfDocs = centroide.getDocsNum().get();
		MapWritable finalMap = centroide.getParole();

		double value;
		Iterator<Entry<Writable, Writable>> iter = finalMap.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Writable, Writable> t = iter.next();
			
			value = ((DoubleWritable)t.getValue()).get() / numOfDocs;
			finalMap.put(t.getKey(), new DoubleWritable(value));
		}
	}
	
	/**
	 * trasforms a MapWritable in a Text
	 * @param finalMap
	 * @return
	 */
	public Text print(int n, MapWritable finalMap){
		String outString = n + ";";

		Iterator<Entry<Writable, Writable>> iter = finalMap.entrySet().iterator();
		//for every word in the map
		while(iter.hasNext()) {
			Entry<Writable, Writable> t = iter.next();
			outString += ((Text)t.getKey()).toString() + ":" + ((DoubleWritable)t.getValue()).get() + ";";
		}
		return new Text(outString);
	}

	@Override
	public void reduce(IntWritable key, Iterable<DocumentWritable> values, Context context) throws IOException, InterruptedException {

		DocumentWritable newCentroid = new DocumentWritable();

		for (DocumentWritable pseudoCentroid : values) {
			newCentroid.merge(pseudoCentroid);
		}
		

		//System.out.println(newCentroid.getDocsNum().get());
		everage(newCentroid);
		Text toReturn=print(newCentroid.getDocsNum().get(), newCentroid.getParole());
		context.write(EMPTY,toReturn);
	}
}
