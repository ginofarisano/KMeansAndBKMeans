package org.soa.kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DocumentWritable implements Writable {

	private MapWritable words = new MapWritable();
	private IntWritable docsNum = new IntWritable(0);

	public MapWritable getWords() {
		return words;
	}

	public void setWords(MapWritable words) {
		this.words = words;
		this.docsNum.set(1);
	}

	public IntWritable getDocsNum() {
		return docsNum;
	}

	public void setDocsNum(IntWritable docsNum) {
		this.docsNum = docsNum;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.words.readFields(arg0);
		this.docsNum.readFields(arg0);

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		this.words.write(arg0);
		this.docsNum.write(arg0);

	}

	public void merge(DocumentWritable doc){

		Iterator<Entry<Writable, Writable>> iter = doc.getWords().entrySet().iterator();

		double value=0;
		double temp=0;

		DoubleWritable ZERO = new DoubleWritable(0);


		while(iter.hasNext()) {
			Entry<Writable, Writable> t = iter.next();
			value = ((DoubleWritable)t.getValue()).get();
			//the intermediate vector has the word in the toMerge vector?
			temp = ((DoubleWritable) this.words.getOrDefault(t.getKey(), ZERO)).get(); 
			//modify or add the value
			value += temp;
			this.words.put(t.getKey(),new DoubleWritable(value));
		}

		//increase the counter
		docsNum.set(docsNum.get()+doc.getDocsNum().get());

	}

	public Text printResult() {
		
		StringBuilder result=new StringBuilder();
		
		result.append("\nThe cluster is composed of "+docsNum.get()+" document\nCentroid:\n");
		
		Iterator<Entry<Writable, Writable>> iter = words.entrySet().iterator();
		
		double value;
		String key;
		
		while(iter.hasNext()) {
			Entry<Writable, Writable> t = iter.next();
			
			value = ((DoubleWritable)t.getValue()).get();
			
			key=((Text)t.getKey()).toString();
			value=((DoubleWritable)t.getValue()).get();
			
			result.append(key+":"+value+"   ");
		}
		
		
		
		return new Text(result.toString());
	}

}
